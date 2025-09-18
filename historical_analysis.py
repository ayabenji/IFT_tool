from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd
import streamlit as st
from openpyxl import load_workbook
from openpyxl.utils.cell import column_index_from_string

from excel_read import _norm


DATE_IN_NAME = re.compile(r"(\d{2})-(\d{2})-(\d{4})")
IFT_FILE_PATTERNS = ("IFT - *.xlsm", "IFT - *.xlsx")
ANALYSIS_SHEET = "IRS - INF – XCCY"
ANALYSIS_HEADER_ROW = 6
ANALYSIS_DIRTY_LETTER = "AN"


def _extract_date_from_name(name: str) -> date | None:
    match = DATE_IN_NAME.search(name)
    if not match:
        return None
    day, month, year = map(int, match.groups())
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _previous_quarter_folder(ifts_dt: date) -> tuple[int, int]:
    quarter = (ifts_dt.month - 1) // 3 + 1
    prev_quarter = quarter - 1
    if prev_quarter == 0:
        prev_quarter = 4
        year = ifts_dt.year - 1
    else:
        year = ifts_dt.year
    quarter_end_months = (3, 6, 9, 12)
    month = quarter_end_months[prev_quarter - 1]
    return year, month


def _iter_existing_files(dirs: Iterable[Path]) -> list[Path]:
    results: list[Path] = []
    for folder in dirs:
        if not folder or not folder.exists():
            continue
        for pattern in IFT_FILE_PATTERNS:
            results.extend(sorted(folder.glob(pattern)))
    return results


def _pick_latest_file(paths: Iterable[Path]) -> Path | None:
    dated: list[tuple[date, Path]] = []
    undated: list[Path] = []
    for path in paths:
        extracted = _extract_date_from_name(path.name)
        if extracted:
            dated.append((extracted, path))
        else:
            undated.append(path)
    if dated:
        dated.sort(key=lambda x: (x[0], x[1].name))
        return dated[-1][1]
    if undated:
        undated.sort()
        return undated[-1]
    return None


def locate_previous_production(base_out: Path, mode: str, ifts_dt: date) -> Path | None:
    year_folder = ifts_dt.strftime("%Y")
    month_folder = ifts_dt.strftime("%m-%Y")
    candidates: list[Path] = []
    if mode.lower() == "close":
        month_dir = base_out / year_folder / month_folder
        candidates.extend(
            [
                month_dir / "prod" / "fast",
                month_dir / "fast",
                month_dir,
            ]
        )
    else:
        prev_year, prev_month = _previous_quarter_folder(ifts_dt)
        prev_month_folder = f"{prev_month:02d}-{prev_year}"
        quarter_dir = base_out / str(prev_year) / prev_month_folder
        candidates.extend(
            [
                quarter_dir / "prod" / "close",
                quarter_dir / "close",
                quarter_dir,
            ]
        )
    paths = _iter_existing_files(candidates)
    return _pick_latest_file(paths)


def guess_current_production(dest_dir: Path, file_tag: str, mode: str) -> Path | None:
    if not dest_dir.exists():
        return None
    base_name = f"IFT_{file_tag}_{mode.lower()}"
    for ext in (".xlsm", ".xlsx"):
        candidate = dest_dir / f"{base_name}{ext}"
        if candidate.exists():
            return candidate
    candidates = sorted(dest_dir.glob("IFT*.xls*"))
    if candidates:
        return candidates[-1]
    return None


def _to_float(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace("\xa0", "").replace(" ", "").strip()
        if not cleaned:
            return None
        if cleaned.count(",") == 1 and cleaned.count(".") == 0:
            cleaned = cleaned.replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def aggregate_dirty_by_classif(path: Path) -> dict[str, float]:
    wb = load_workbook(path, data_only=True, read_only=True)
    if ANALYSIS_SHEET not in wb.sheetnames:
        raise KeyError(f"Feuille '{ANALYSIS_SHEET}' absente de {path.name}")
    ws = wb[ANALYSIS_SHEET]
    classif_col = None
    max_col = ws.max_column
    for col in range(1, max_col + 1):
        header_val = ws.cell(row=ANALYSIS_HEADER_ROW, column=col).value
        if header_val is None:
            continue
        if _norm(header_val) == _norm("Classif DI"):
            classif_col = col
            break
    if classif_col is None:
        raise KeyError("Colonne 'Classif DI' introuvable")
    dirty_idx = column_index_from_string(ANALYSIS_DIRTY_LETTER)
    totals: dict[str, float] = {}
    for row in ws.iter_rows(
        min_row=ANALYSIS_HEADER_ROW + 1,
        max_row=ws.max_row,
        values_only=True,
    ):
        if classif_col - 1 >= len(row) or dirty_idx - 1 >= len(row):
            continue
        classif_raw = row[classif_col - 1]
        dirty_raw = row[dirty_idx - 1]
        if classif_raw is None:
            continue
        classif = str(classif_raw).strip()
        if not classif:
            continue
        dirty = _to_float(dirty_raw)
        if dirty is None:
            continue
        totals[classif] = totals.get(classif, 0.0) + dirty
    wb.close()
    return totals


def build_comparison_dataframe(
    current_map: dict[str, float], previous_map: dict[str, float]
) -> pd.DataFrame:
    keys = sorted({*current_map.keys(), *previous_map.keys()})
    rows = []
    for key in keys:
        old_val = previous_map.get(key, 0.0)
        new_val = current_map.get(key, 0.0)
        delta = new_val - old_val
        pct = (delta / old_val * 100.0) if abs(old_val) > 1e-12 else None
        rows.append(
            {
                "Classif DI": key,
                "Dirty ancienne prod": old_val,
                "Dirty prod actuelle": new_val,
                "Écart": delta,
                "Écart %": pct,
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("Écart", key=lambda s: s.abs(), ascending=False)
    return df


def render_analysis_tab(
    base_out: Path,
    dest_dir: Path,
    mode: str,
    ifts_date: date,
    file_tag: str,
) -> None:
    st.write(
        "Comparer la production actuelle avec la précédente pour identifier les écarts de dirty value par `Classif DI`."
    )

    previous_guess = locate_previous_production(base_out, mode, ifts_date)
    current_guess = guess_current_production(dest_dir, file_tag, mode)
    current_session_path = Path(st.session_state.get("out_xlsm", "")) if "out_xlsm" in st.session_state else None
    if current_session_path and current_session_path.exists():
        current_guess = current_session_path

    current_path_str = st.text_input(
        "Fichier de prod actuel (.xlsm)",
        value=str(current_guess) if current_guess else "",
        placeholder="Chemin complet du fichier courant",
    )
    previous_path_str = st.text_input(
        "Ancienne production à comparer",
        value=str(previous_guess) if previous_guess else "",
        placeholder="Chemin complet du fast/close précédent",
    )

    if st.button("🔍 Lancer l'analyse comparative"):
        try:
            if not current_path_str:
                st.error("Indique le fichier de production actuel à analyser.")
                st.stop()
            if not previous_path_str:
                st.error("Indique le fichier de l'ancienne production (fast/close).")
                st.stop()
            current_path = Path(current_path_str)
            previous_path = Path(previous_path_str)
            if not current_path.exists():
                st.error(f"Fichier actuel introuvable : {current_path}")
                st.stop()
            if not previous_path.exists():
                st.error(f"Ancienne production introuvable : {previous_path}")
                st.stop()

            st.write(f"Fichier actuel : **{current_path.name}**")
            st.write(f"Ancienne prod : **{previous_path.name}**")

            current_totals = aggregate_dirty_by_classif(current_path)
            previous_totals = aggregate_dirty_by_classif(previous_path)

            df_compare = build_comparison_dataframe(current_totals, previous_totals)
            if df_compare.empty:
                st.warning("Aucune donnée agrégée — vérifier la feuille ou les colonnes cibles.")
                st.stop()

            total_current = sum(current_totals.values())
            total_previous = sum(previous_totals.values())
            delta_total = total_current - total_previous
            col_cur, col_prev = st.columns(2)
            with col_cur:
                st.metric(
                    "Total dirty actuel",
                    f"{total_current:,.2f}",
                    delta=f"{delta_total:,.2f}",
                )
            with col_prev:
                st.metric("Total dirty ancien", f"{total_previous:,.2f}")

            st.dataframe(df_compare)

            csv_bytes = df_compare.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ Exporter la comparaison (CSV)",
                data=csv_bytes,
                file_name="comparaison_dirty_classif.csv",
                mime="text/csv",
            )
        except Exception as exc:
            st.exception(exc)