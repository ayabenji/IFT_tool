# ──────────────────────────────────────────────────────────────────────────────
# Project: IFT Downloader & Extractor (refactor v1)
# Modules layout (put each into its own file in the same folder):
#   - app.py                (Streamlit UI & orchestration)
#   - io_zip.py             (filesystem helpers & zip extraction)
#   - excel_read.py         (smart Excel reading, header detect, column tools)
#   - perimeter.py          (perimeter construction)
#   - template_write.py     (template copy & header index)
#   - yaml_apply.py         (YAML parsing, preview, integration)
# Requirements (conda/pip): streamlit, pandas, openpyxl, xlrd (<=1.2.0), pyyaml, xlsxwriter
# Notes:
#  - Per tes consignes: on enlève calamine. .xls via xlrd, .xlsx via openpyxl.
#  - Casts nombres/dates avant écriture.
#  - Lookup 'source' YAML tolérant via matching normalisé comme get_col.
#  - Les % total restent divisés par notional_leg1 (choix assumé).
# ──────────────────────────────────────────────────────────────────────────────

# ============================ file: io_zip.py =================================
from __future__ import annotations
from pathlib import Path
from datetime import date, datetime, timedelta
import zipfile
import shutil

__all__ = [
    "next_business_day", "ensure_dir", "unique_path", "extract_xls_from_zip",
]

def next_business_day(d: date) -> date:
    """Return the next business day (Mon–Fri), ignoring public holidays."""
    nd = d + timedelta(days=1)
    while nd.weekday() >= 5:  # 5=Sat, 6=Sun
        nd += timedelta(days=1)
    return nd


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def unique_path(dest_dir: Path, filename: str) -> Path:
    """Return a non-colliding path in dest_dir for filename, adding _1, _2, ... if needed."""
    base = Path(filename).stem
    ext = Path(filename).suffix
    candidate = dest_dir / filename
    i = 1
    while candidate.exists():
        candidate = dest_dir / f"{base}_{i}{ext}"
        i += 1
    return candidate


def extract_xls_from_zip(zip_path: Path, dest_dir: Path) -> list[Path]:
    """Extract only .xls (and .xlsx) files from a ZIP to dest_dir (flattened), avoid overwriting.
    Returns list of extracted paths.
    """
    extracted: list[Path] = []
    with zipfile.ZipFile(zip_path, "r") as z:
        for info in z.infolist():
            name = info.filename
            if (name.lower().endswith(".xls") or name.lower().endswith(".xlsx")) and not name.endswith("/"):
                target = unique_path(dest_dir, Path(name).name)  # flatten
                with z.open(info) as src, open(target, "wb") as out:
                    shutil.copyfileobj(src, out)
                extracted.append(target)
    return extracted

