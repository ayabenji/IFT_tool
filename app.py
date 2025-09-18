from __future__ import annotations
import streamlit as st
from datetime import date
from pathlib import Path

import pandas as pd
from io import BytesIO
import re

from io_zip import (next_business_day)
from historical_analysis import render_analysis_tab
from workflow_tab import render_workflow_tab



# --- App config ---
st.set_page_config(page_title="IFT prod", page_icon="📦", layout="centered")
st.title("📦 IFT production tool")

# --- UI: Mode & Date ---
col_mode, col_date = st.columns(2)
with col_mode:
    mode = st.radio("Mode", ["Fast", "Close"], horizontal=True, index=0)
with col_date:
    ifts_date = st.date_input("Date des IFTS", value=date.today())

file_day = next_business_day(ifts_date)
file_tag = file_day.strftime("%m%d%Y")

st.info(f"Jour ouvré utilisé : **{file_day.strftime('%A %d %B %Y')}** → tag **{file_tag}**")

# --- Paths (constants per tes consignes) ---
src_dir = Path(r"S:\PRD\SuperDerivatives\In\Archives")
base_out = Path(r"C:\Users\abenjelloun\OneDrive - Cooperactions\GAM-E-Risk Perf - RMP\1.PROD\4.REPORTINGS SPEC CLIENTS\1.Groupe - IFT (CB-JB)")

year_folder = ifts_date.strftime("%Y")
month_folder = ifts_date.strftime("%m-%Y")
dest_dir = base_out / year_folder / month_folder / "prod" / mode.lower()

with st.expander("Chemins (lecture seule)"):
    st.code(f"Source: {src_dir}\nDestination: {dest_dir}")

# --- Patterns de recherche ---
patterns = [
    f"XCY_IR_{file_tag}*.zip",
    f"IR_{file_tag}*.zip",
]

tab_workflow, tab_analysis = st.tabs(["Production IFT", "Analyse ancienne prod"])
with tab_workflow:
    render_workflow_tab(src_dir, dest_dir, patterns, file_tag, mode, ifts_date)
with tab_analysis:
    render_analysis_tab(base_out, dest_dir, mode, ifts_date, file_tag)