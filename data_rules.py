from __future__ import annotations
import re
import pandas as pd
import numpy as np

__all__ = ["valid_code_mask"]

_INVALID_CODE_RE = re.compile(r"(?i)^(nan|none|null|na|n/?a|#n/?a|-|—)$")

def valid_code_mask(s: pd.Series) -> pd.Series:
    """Mask valide pour Code DI: exclut NaN réels, blancs et sentinelles usuelles."""
    s_isna = s.isna()
    s_str = s.astype(str).str.replace(" ", " ", regex=False).str.strip()
    s_bad = s_str.eq("") | s_str.str.fullmatch(_INVALID_CODE_RE)
    return (~s_isna) & (~s_bad)
