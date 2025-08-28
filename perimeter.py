from __future__ import annotations
import pandas as pd
from typing import Sequence
from excel_read import get_col, label_duplicate_columns
from data_rules import valid_code_mask

__all__ = ["build_perimeter"]


def build_perimeter(frames: Sequence[pd.DataFrame]) -> tuple[pd.DataFrame, dict]:
    """Concat, normalise, filtre Code DI, déduplique, renvoie (result, meta)."""
    raw = pd.concat(frames, ignore_index=True, sort=False)
    # Colonnes utiles
    col_code_di = get_col(raw, "Custom Attribute5 Value")
    col_class = get_col(raw, "Class")

    # Identifiants de secours
    id_cols: list[str] = []
    for k in ["#Ticket", "Trade ID", "External Id"]:
        c = get_col(raw, k, required=False)
        if c:
            id_cols.append(c)

    # Filtre Code DI
    mask = valid_code_mask(raw[col_code_di])
    peri = raw[mask].copy()
    # Option: fige "Code DI" en texte pour préserver d'éventuels zéros à gauche
    peri[col_code_di] = peri[col_code_di].astype(str).str.strip()

    out_cols = {
        "Code DI": col_code_di,
        "Classif DI": col_class,
        "Class": col_class,
    }
    keep_meta = [c for c in id_cols if c]
    for k in ["Counterparty", "Currency"]:
        c = get_col(raw, k, required=False)
        if c:
            keep_meta.append(c)

    result = pd.DataFrame({dst: peri[src] for dst, src in out_cols.items()})
    # Préserver le format texte du Code DI
    result["Code DI"] = result["Code DI"].astype(str).str.strip()
    for c in keep_meta:
        result[c] = peri[c]

    before = len(result)
    if id_cols:
        result = result.drop_duplicates(subset=id_cols)
    after = len(result)

    meta = {
        "dedup_keys": id_cols,
        "dedup_before": before,
        "dedup_after": after,
    }
    return result, meta
