from __future__ import annotations
from pathlib import Path
from typing import Iterable
import pandas as pd

__all__ = [
    "_norm", "EXPECTED_HEADERS", "read_xls_smart", "read_xls_with_positions", "label_duplicate_columns", "get_col",
]


def _norm(s: str) -> str:
    return " ".join(str(s).strip().lower().split())

# headers attendus (normalisés)
EXPECTED_HEADERS = {
    _norm(x) for x in [
        "#Ticket","Trade ID","External Id","Counterparty","Currency","Class",
        "Custom Attribute5 Value","Leg Type","Pay/Rec","Index/Fixed Rate",
        "Spread (bp)","Start Date","End Date","Notional",
        "Dirty Value","Clean Value","Accrued Interest",
    ]
}


def _score_header_row(df_nohdr: pd.DataFrame, ridx: int) -> int:
    vals = [_norm(v) for v in df_nohdr.iloc[ridx].tolist()]
    return sum(1 for v in vals if v in EXPECTED_HEADERS)


def _flatten_two_rows(r1: Iterable, r2: Iterable) -> list[str]:
    r1, r2 = list(r1), list(r2)
    ln = max(len(r1), len(r2))
    out: list[str] = []
    for i in range(ln):
        a = str(r1[i]) if i < len(r1) and r1[i] is not None else ""
        b = str(r2[i]) if i < len(r2) and r2[i] is not None else ""
        out.append((b.strip() or a.strip()))
    return out


def _read_with_engine(path: Path, engine: str | None) -> pd.DataFrame:
    # Pas de dtype=str: on garde les types natifs pour les nombres/dates
    return pd.read_excel(path, sheet_name=0, header=None, engine=engine)


def read_xls_smart(path: Path, search_rows: int = 12) -> pd.DataFrame:
    """Lit un .xls/.xlsx, détecte la meilleure ligne d'entête (1 ou 2 lignes) et retourne le corps typé.
    Priorité moteurs: xlrd (xls), openpyxl (xlsx). On enlève calamine.
    """
    last_err: Exception | None = None
    suffix = path.suffix.lower()
    engines = ["xlrd"] if suffix == ".xls" else ["openpyxl", None]
    for engine in engines:
        try:
            df0 = _read_with_engine(path, engine)
        except Exception as e:
            last_err = e
            continue
        # Chercher la meilleure ligne d'entête
        best = (-1, -1)
        top = min(search_rows, len(df0))
        for r in range(top):
            s = _score_header_row(df0, r)
            if s > best[0]:
                best = (s, r)
        if best[1] == -1:
            hdr_idx = 5
            cols = df0.iloc[hdr_idx].astype(str).str.strip().tolist()
            body_full = df0.iloc[hdr_idx+1:].copy()
        else:
            ridx = best[1]
            single_cols = df0.iloc[ridx].astype(str).str.strip().tolist()
            two_cols = _flatten_two_rows(df0.iloc[ridx], df0.iloc[ridx+1] if ridx+1 < len(df0) else [])
            single_score = sum(1 for v in single_cols if _norm(v) in EXPECTED_HEADERS)
            two_score = sum(1 for v in two_cols if _norm(v) in EXPECTED_HEADERS)
            if two_score > single_score:
                cols = two_cols
                body_full = df0.iloc[ridx+2:].copy()
            else:
                cols = single_cols
                body_full = df0.iloc[ridx+1:].copy()
        # Nettoyage colonnes
        cols = [(c if c and not str(c).startswith("Unnamed") else "") for c in cols]
        body = body_full.copy()
        body.columns = cols
        body = body.dropna(axis=1, how="all")
        return body
    raise RuntimeError(f"Impossible de lire {path.name}. Dernière erreur: {last_err}")

from openpyxl.utils import get_column_letter

def read_xls_with_positions(path: Path, search_rows: int = 12) -> tuple[pd.DataFrame, dict[str, str]]:
    """Comme read_xls_smart mais retourne aussi un mapping **lettre Excel → nom de colonne DataFrame**
    après normalisation/suffixage. Ce mapping conserve la position d'origine même si on supprime
    des colonnes vides avant le corps.
    """
    last_err: Exception | None = None
    suffix = path.suffix.lower()
    engines = ["xlrd"] if suffix == ".xls" else ["openpyxl", None]
    for engine in engines:
        try:
            df0 = _read_with_engine(path, engine)
        except Exception as e:
            last_err = e
            continue
        # Best header row
        best = (-1, -1)
        top = min(search_rows, len(df0))
        for r in range(top):
            s = _score_header_row(df0, r)
            if s > best[0]:
                best = (s, r)
        if best[1] == -1:
            hdr_idx = 5
            header_row = df0.iloc[hdr_idx]
            body_full = df0.iloc[hdr_idx+1:].copy()
            cols_single = header_row.astype(str).str.strip().tolist()
            cols_used = cols_single
            start_body_idx = hdr_idx+1
        else:
            ridx = best[1]
            header1 = df0.iloc[ridx]
            header2 = df0.iloc[ridx+1] if ridx+1 < len(df0) else None
            single_cols = header1.astype(str).str.strip().tolist()
            two_cols = _flatten_two_rows(header1, header2 if header2 is not None else [])
            single_score = sum(1 for v in single_cols if _norm(v) in EXPECTED_HEADERS)
            two_score = sum(1 for v in two_cols if _norm(v) in EXPECTED_HEADERS)
            if two_score > single_score:
                cols_used = two_cols
                body_full = df0.iloc[ridx+2:].copy()
                start_body_idx = ridx+2
            else:
                cols_used = single_cols
                body_full = df0.iloc[ridx+1:].copy()
                start_body_idx = ridx+1
        # Build keep mask (non all-null) per original position
        keep_idx: list[int] = []
        clean_names: list[str] = []
        for j in range(len(cols_used)):
            col_name = cols_used[j]
            col_name = col_name if col_name and not str(col_name).startswith("Unnamed") else ""
            col_series = body_full.iloc[:, j] if j < body_full.shape[1] else pd.Series([], dtype=object)
            if col_series.shape[0] == 0 or col_series.notna().any():
                # keep column (even if unnamed, it might be needed to maintain positions)
                # but we'll drop truly all-null unnamed later when assigning dataframe
                keep_idx.append(j)
                clean_names.append(col_name)
        # Create body_kept with kept indices
        body_kept = body_full.iloc[:, keep_idx].copy()
        body_kept.columns = clean_names
        # Drop columns that are completely empty *and* unnamed
        mask_drop = [(c == "" and body_kept[c].isna().all()) for c in body_kept.columns]
        if any(mask_drop):
            body_kept = body_kept.loc[:, [c for c, drop in zip(body_kept.columns, mask_drop) if not drop]]
            keep_idx = [idx for idx, drop in zip(keep_idx, mask_drop) if not drop]
        # Label duplicates
        body_labeled = label_duplicate_columns(body_kept)
        # Build letter map based on original positions for kept columns
        letter_map: dict[str, str] = {}
        for j, colname in zip(keep_idx, body_labeled.columns):
            letter = get_column_letter(j + 1)
            letter_map[letter.upper()] = colname
        return body_labeled, letter_map
    raise RuntimeError(f"Impossible de lire {path.name}. Dernière erreur: {last_err}")


def label_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(df.columns)
    base_counts: dict[str, int] = {}
    new_cols: list[str] = []
    for c in cols:
        if isinstance(c, str) and "." in c and c.split(".")[-1].isdigit():
            base, idx = c.rsplit(".", 1)
            leg = int(idx) + 1
            new_cols.append(f"{base} (Leg{leg})")
        else:
            base = str(c)
            if base in base_counts:
                base_counts[base] += 1
                new_cols.append(f"{base} (Leg{base_counts[base]+1})")
            else:
                base_counts[base] = 0
                new_cols.append(base)
    out = df.copy()
    out.columns = new_cols
    return out


def get_col(df: pd.DataFrame, logical_name: str, required: bool = True) -> str | None:
    target = _norm(logical_name)
    mapping = {c: _norm(str(c)) for c in df.columns}
    for c, n in mapping.items():
        if n == target:
            return c
    if required:
        raise KeyError(f"Colonne introuvable: '{logical_name}' dans {list(df.columns)[:8]}… (total {len(df.columns)})")
    return None
