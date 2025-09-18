from __future__ import annotations

import pandas as pd
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, Tuple, TYPE_CHECKING

from openpyxl import load_workbook

from excel_read import _norm
from template_write import build_targets_index, letter_to_index

__all__ = [
    "expected_sensis_name",
    "locate_sensis_file",
    "load_sensis_table",
    "apply_sensis_to_workbook",
]


@dataclass
class SensisEntry:
    sensis_leg1: float | None
    sensis_leg2: float | None
    duration_leg1: float | None
    duration_leg2: float | None
    duration_total: float | None
    sensis_total_value: float | None

    @property
    def sensis_total(self) -> float | None:
        if self.sensis_total_value is not None:
            return self.sensis_total_value
        vals = [v for v in (self.sensis_leg1, self.sensis_leg2) if v is not None]
        if not vals:
            return None
        return sum(vals)




def expected_sensis_ir_prefix(ifts_date: date) -> str:
    return f"sensis_IR_*"


def locate_sensis_file(dest_dir: Path, ifts_date: date) -> Path:

    ir_prefix = expected_sensis_ir_prefix(ifts_date)
    pattern_ir = f"[sS]ensis_IR_*.xls*"
    matches = sorted(dest_dir.glob(pattern_ir))
    if matches:
        return matches[0]
    raise FileNotFoundError(
        f"Fichier Sensis introuvable dans {dest_dir}: {pattern_ir}"
    )
    


def _normalize_header(value: object) -> str:
    text = " ".join(str(value).strip().lower().split()) if value is not None else ""
    text = (
        text.replace("(", " ")
        .replace(")", " ")
        .replace("/", " ")
        .replace("%", " %")
    )
    text = " ".join(text.split())
    return text


_HEADER_ALIASES: Dict[str, Tuple[str, ...]] = {
    "code": ("code di",),
    "notional": ("notional",)
}





def _find_header_row(df: pd.DataFrame, search_rows: int = 12) -> int:
    max_row = min(len(df), search_rows)
    max_col = df.shape[1]
    for row in range(max_row):
        values = df.iloc[row, :max_col].tolist()
        normed = {
            _normalize_header(v)
            for v in values
            if v is not None and not pd.isna(v)
        }
        if any(alias in normed for alias in ("code di", "code")):
            return row
    return 3


def _build_header_map(df: pd.DataFrame, header_row: int) -> Dict[str, int]:
    mapping: Dict[str, int] = {}
    for col in range(df.shape[1]):
        value = df.iat[header_row, col]
        if value is None or pd.isna(value):
            continue
        key = _normalize_header(value)
        if key and key not in mapping:
            mapping[key] = col
    return mapping


def _column_for(header_map: Dict[str, int], logical: str) -> int | None:
    aliases = _HEADER_ALIASES.get(logical, ())
    for alias in aliases:
        if alias in header_map:
            return header_map[alias]
    return None


def load_sensis_table(path: Path) -> Dict[str, SensisEntry]:
    suffix = path.suffix.lower()
    if suffix == ".xls":
        engine = "xlrd"
    elif suffix in {".xlsx", ".xlsm"}:
        engine = "openpyxl"
    else:
        engine = None
    df = pd.read_excel(path, header=None, engine=engine)



    header_row = _find_header_row(df)
    header_map = _build_header_map(df, header_row)

    col_code = _column_for(header_map, "code")
    if col_code is None:
        raise KeyError("Colonne 'Code DI' introuvable dans Sensis")

    col_sensis1 = letter_to_index("X")
    col_dur1 = letter_to_index("Y")
    col_dur2 = letter_to_index("AC")
    col_sensis2 = letter_to_index("AD")
    col_duration_total = letter_to_index("AL")
    col_sensis_total = letter_to_index("AH")

    def _value(row_idx: int, col_idx: int | None) -> object | None:
        if col_idx is None:
            return None
        if col_idx >= df.shape[1]:
            return None
        value = df.iat[row_idx, col_idx]
        if value is None or pd.isna(value):
            return None
        return value

    data: Dict[str, SensisEntry] = {}
    for row in range(header_row + 1, len(df)):
        code_cell = _value(row, col_code) 
        if code_cell is None:
                continue
        code = str(code_cell).strip()
        if not code:
            continue

        entry = SensisEntry(
            sensis_leg1=_value(row, col_sensis1),
            sensis_leg2=_value(row, col_sensis2),
            duration_leg1=_value(row, col_dur1),
            duration_leg2=_value(row, col_dur2),
            duration_total=_value(row, col_duration_total),
            sensis_total_value=_value(row, col_sensis_total),
        )
        data[code] = entry
    return data


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


def _mul(a: object, b: object) -> float | None:
    fa = _to_float(a)
    fb = _to_float(b)
    if fa is None or fb is None:
        return None
    return fa * fb


def _sum_optional(values: Iterable[object]) -> float | None:
    collected = [v for v in (_to_float(x) for x in values) if v is not None]
    if not collected:
        return None
    return sum(collected)


def _sub_optional(left: object, right: object) -> float | None:
    lf = _to_float(left)
    rf = _to_float(right)
    if lf is None and rf is None:
        return None
    return (lf or 0.0) - (rf or 0.0)


def apply_sensis_to_workbook(
    workbook_path: Path,
    sensis: Dict[str, SensisEntry],
    header_row: int = 6,
    sheet: str = "IRS - INF – XCCY",
) -> Tuple[int, list[str], list[dict[str, object]]]:
    if not workbook_path.exists():
        raise FileNotFoundError(workbook_path)
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
    missing_codes: list[str] = []
    updated_rows: list[dict[str, object]] = []

    col_notional = letter_to_index("M") + 1

    for row in range(start_row, ws.max_row + 1):
        code_cell = ws.cell(row=row, column=code_col).value
        if code_cell is None or (isinstance(code_cell, str) and not code_cell.strip()):
            # On s'arrête dès la première ligne vide dans la colonne Code DI
            break
        code = str(code_cell).strip()
        entry = sensis.get(code)
        if entry is None:
            missing_codes.append(code)
            continue

        notional = ws.cell(row=row, column=col_notional).value

        def set_value(letter: str, value: object) -> None:
            idx = letter_to_index(letter) + 1
            ws.cell(row=row, column=idx).value = value


        set_value("T", entry.duration_leg1)
        set_value("U", entry.sensis_leg1)
        set_value("AK", entry.duration_leg2)
        set_value("AL", entry.sensis_leg2)
        set_value("AT", entry.duration_total)
        set_value("AU", entry.sensis_total)

        def get_value(letter: str) -> object:
            idx = letter_to_index(letter) + 1
            return ws.cell(row=row, column=idx).value

       

        updated += 1
        updated_rows.append(
            {
                "Code DI": code,
                "Sensis L1": entry.sensis_leg1,
                "Sensis L2": entry.sensis_leg2,
                "Duration L1": entry.duration_leg1,
                "Duration L2": entry.duration_leg2,
                "Duration Totale": entry.duration_total,
                "Sensis Totale": entry.sensis_total
            }
        )

    wb.save(workbook_path)
    return updated, missing_codes, updated_rows