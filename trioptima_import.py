from __future__ import annotations

import importlib
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple

try:  # Pandas peut être absent au démarrage (chargement paresseux)
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - fallback runtime
    pd = None  # type: ignore[assignment]

from openpyxl import load_workbook

from excel_read import _norm
from template_write import build_targets_index, letter_to_index

__all__ = [
    "expected_trioptima_prefix",
    "locate_trioptima_file",
    "load_trioptima_table",
    "aggregate_trioptima",
    "build_trioptima_mapping",
    "apply_trioptima_to_workbook",
]


def _ensure_pandas() -> "pd":
    global pd
    if pd is None:
        pd = importlib.import_module("pandas")  # type: ignore[assignment]
    return pd  # type: ignore[return-value]


def expected_trioptima_prefix(ifts_date: date) -> str:
    """Préfixe attendu pour le fichier TriOptima à partir de la date de prod."""

    return f"search_groupama-am_{ifts_date:%Y-%m-%d}"


def locate_trioptima_file(prod_dir: Path, ifts_date: date) -> Path:
    """Localise le fichier TriOptima à partir du préfixe attendu."""

    prefix = expected_trioptima_prefix(ifts_date)
    direct_pattern = f"{prefix}*.csv"
    matches = sorted(prod_dir.glob(direct_pattern))
    if matches:
        return matches[0]

    prefix_lower = prefix.lower()
    fallback = [
        path
        for path in sorted(prod_dir.glob("*.csv"))
        if path.name.lower().startswith(prefix_lower)
    ]
    if fallback:
        return fallback[0]

    raise FileNotFoundError(
        f"Fichier TriOptima introuvable dans {prod_dir}: {direct_pattern}"
    )


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("\u00a0", " ")
    if not text:
        return None
    text = text.replace(" ", "").replace("'", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _sub_optional(left: object, right: object) -> float | None:
    lf = _to_float(left)
    rf = _to_float(right)
    if lf is None and rf is None:
        return None
    return (lf or 0.0) - (rf or 0.0)


def _div_optional(numerator: object, denominator: object) -> float | None:
    num = _to_float(numerator)
    den = _to_float(denominator)
    if num is None or den is None or den == 0:
        return None
    return num / den


def load_trioptima_table(path: Path) -> "pd.DataFrame":
    """Charge le CSV TriOptima et prépare les colonnes nécessaires."""

    _ensure_pandas()
    df = pd.read_csv(path,decimal='.',sep=';')  # type: ignore[attr-defined]

    required = {"FREE_TEXT_2", "MTM_VALUE", "MTM_DIFF"}
    missing = required.difference(df.columns)
    if missing:
        raise KeyError(
            f"Colonnes manquantes dans {path.name}: {', '.join(sorted(missing))}"
        )

    df = df.copy()
    df = df[df["FREE_TEXT_2"].notna()]
    df["FREE_TEXT_2"] = df["FREE_TEXT_2"].astype(str).str.strip()
    df = df[df["FREE_TEXT_2"] != ""]

    codes = df["FREE_TEXT_2"].str.split("/", n=1).str[0].str.strip()
    df["Code DI"] = codes
    df = df[df["Code DI"] != ""]

    for column in ("MTM_VALUE", "MTM_DIFF"):
        df[column] = df[column].apply(_to_float)

    df["MTM_VALUE"] = df["MTM_VALUE"].fillna(0.0)
    df["MTM_DIFF"] = df["MTM_DIFF"].fillna(0.0)
    df["MTM_CONTREPARTIE"] = df["MTM_VALUE"] - df["MTM_DIFF"]

    return df[["Code DI", "MTM_VALUE", "MTM_DIFF", "MTM_CONTREPARTIE"]]


def aggregate_trioptima(df: "pd.DataFrame") -> "pd.DataFrame":
    """Agrège les montants TriOptima par Code DI."""

    _ensure_pandas()
    if df.empty:
        return df.copy()

    df = df.copy()
    numeric_cols = ["MTM_VALUE", "MTM_DIFF", "MTM_CONTREPARTIE"]
    for column in numeric_cols:
        if column in df.columns:
            df[column] = df[column].fillna(0.0)

    grouped = (
        df.groupby("Code DI", as_index=False)[numeric_cols]
        .sum()
        .sort_values("Code DI")
    )
    return grouped


def build_trioptima_mapping(df: "pd.DataFrame") -> Dict[str, float]:
    """Construit un dictionnaire Code DI → MTM contrepartie agrégé."""

    mapping: Dict[str, float] = {}
    _ensure_pandas()
    if df.empty:
        return mapping
    for _, row in df.iterrows():
        code = str(row["Code DI"]).strip()
        if not code:
            continue
        value = row.get("MTM_CONTREPARTIE")
        if pd.isna(value):  # type: ignore[arg-type]
            continue
        value_float = _to_float(value)
        if value_float is None:
            continue
        mapping[code] = value_float
    return mapping


def apply_trioptima_to_workbook(
    workbook_path: Path,
    mtm_mapping: Dict[str, float],
    *,
    header_row: int = 6,
    sheet: str = "IRS - INF – XCCY",
) -> Tuple[int, List[str], List[dict[str, object]], List[str]]:
    """Injecte les MTM contrepartie dans le template et recalcule les écarts."""

    if not workbook_path.exists():
        raise FileNotFoundError(workbook_path)
    if not mtm_mapping:
        return 0, [], [], []

    keep_vba = workbook_path.suffix.lower() == ".xlsm"
    wb = load_workbook(workbook_path, keep_vba=keep_vba, data_only=False)
    if sheet not in wb.sheetnames:
        raise KeyError(f"Feuille '{sheet}' absente de {workbook_path.name}")
    ws = wb[sheet]

    targets = build_targets_index(ws, header_row)
    code_cols = targets.get(_norm("Code DI"), [])
    if not code_cols:
        raise KeyError("Colonne 'Code DI' introuvable dans le template")
    code_col = code_cols[0]

    start_row = header_row + 1
    updated = 0
    missing_codes: List[str] = []
    used_codes: set[str] = set()
    preview: List[dict[str, object]] = []

    col_notional = letter_to_index("M") + 1

    def _cell_value(letter: str, row_idx: int) -> object:
        idx = letter_to_index(letter) + 1
        return ws.cell(row=row_idx, column=idx).value

    def _set_value(letter: str, row_idx: int, value: object) -> None:
        idx = letter_to_index(letter) + 1
        ws.cell(row=row_idx, column=idx).value = value

    for row in range(start_row, ws.max_row + 1):
        code_cell = ws.cell(row=row, column=code_col).value
        if code_cell is None or (isinstance(code_cell, str) and not code_cell.strip()):
            break
        code = str(code_cell).strip()
        mtm_value = mtm_mapping.get(code)
        if mtm_value is None:
            missing_codes.append(code)
            continue

        used_codes.add(code)

        an_value = _cell_value("AN", row)
        notional = ws.cell(row=row, column=col_notional).value

        bd_value = _sub_optional(an_value, mtm_value)
        be_value = _div_optional(bd_value, notional)
        ax_value = _div_optional(mtm_value, notional)

        _set_value("AW", row, mtm_value)
        _set_value("AX", row, ax_value)
        _set_value("BD", row, bd_value)
        _set_value("BE", row, be_value)

        # Recalcule les colonnes de comparaison dépendantes
        _set_value("BK", row, _sub_optional(an_value, mtm_value))
        _set_value("BS", row, _sub_optional(an_value, _cell_value("BD", row)))
        _set_value("BT", row, _sub_optional(_cell_value("AO", row), _cell_value("BE", row)))
        _set_value(
            "CA",
            row,
            _sub_optional(_cell_value("AW", row), _cell_value("BD", row)),
        )
        _set_value(
            "CB",
            row,
            _sub_optional(_cell_value("AX", row), _cell_value("BE", row)),
        )

        updated += 1
        preview.append(
            {
                "Code DI": code,
                "MTM Contrepartie": mtm_value,
                "Diff (AN-AW)": bd_value,
                "Diff / Notional": be_value,
            }
        )

    wb.save(workbook_path)

    unused_codes = sorted(set(mtm_mapping).difference(used_codes))
    return updated, missing_codes, preview, unused_codes