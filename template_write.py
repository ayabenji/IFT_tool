
# ======================== file: template_write.py =============================
from __future__ import annotations
from pathlib import Path
import shutil
from openpyxl.utils.cell import column_index_from_string

__all__ = ["copy_template_to_dest", "build_targets_index", "letter_to_index"]


def copy_template_to_dest(template_path: Path, dest_dir: Path, file_tag: str, mode: str) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    ext = template_path.suffix  # conserve .xlsx ou .xlsm
    out_path = dest_dir / f"IFT_{file_tag}_{mode.lower()}{ext}"
    shutil.copy2(template_path, out_path)
    return out_path


def build_targets_index(ws, header_row: int) -> dict[str, list[int]]:
    idx: dict[str, list[int]] = {}
    for c in range(1, ws.max_column + 1):
        v = ws.cell(row=header_row, column=c).value
        if v is None:
            continue
        k = str(v).strip().lower()
        k = " ".join(k.split())
        idx.setdefault(k, []).append(c)
    return idx

def letter_to_index(letter: str) -> int:
    return column_index_from_string(letter) - 1