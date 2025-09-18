from __future__ import annotations
import streamlit as st
from pathlib import Path
from datetime import date
import pandas as pd
from io import BytesIO

from io_zip import next_business_day, ensure_dir, extract_xls_from_zip
from excel_read import read_xls_smart, read_xls_with_positions, label_duplicate_columns, _norm
from perimeter import build_perimeter
from yaml_apply import load_cfg, preview_yaml_rows, integrate_yaml_to_template
from mail_outlook import build_ifts_filename, export_xlsx_copy, prepare_outlook_draft, prepare_trioptima_request_mail, prepare_collateral_report_request_mail
from sensis_import import (
    locate_sensis_file,
    load_sensis_table,
    apply_sensis_to_workbook,
    expected_sensis_name,
)
# --- App config ---
st.set_page_config(page_title="IFT Downloader", page_icon="📦", layout="centered")
st.title("📦 IFT Downloader & Extractor (refactor v1)")

# --- UI: Mode & Date ---
col1, col2 = st.columns(2)
with col1:
    mode = st.radio("Mode", ["Fast", "Close"], horizontal=True, index=0)
with col2:
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

# --- Step 1: Recherche & extraction ---
if st.button("🔎 Rechercher & extraire"):
    try:
        if not src_dir.exists():
            st.error(f"Dossier source introuvable: {src_dir}")
            st.stop()
        ensure_dir(dest_dir)
        found = []
        for pat in patterns:
            matches = list(src_dir.glob(pat))
            if matches:
                st.write(f"Pattern **{pat}** → {len(matches)} fichier(s) trouvé(s)")
            else:
                st.warning(f"Pattern **{pat}** → aucun fichier trouvé")
            found.extend(matches)
        if not found:
            st.stop()

        total_extracted = []
        for zp in sorted(found):
            st.write(f"➡️ Traitement du zip : {zp.name}")
            extracted = extract_xls_from_zip(zp, dest_dir)
            if extracted:
                st.success(f"{len(extracted)} fichier(s) .xls/.xlsx extrait(s)")
                for p in extracted:
                    st.write(f"• {p.name}")
                total_extracted.extend(extracted)
            else:
                st.warning("Aucun .xls/.xlsx trouvé dans cette archive")
        if total_extracted:
            st.success(f"✅ Terminé. Fichiers extraits dans : {dest_dir}")
        else:
            st.warning("Aucun fichier extrait. Vérifier les archives et le tag de date.")
    except Exception as e:
        st.exception(e)

# ===================== Étape 2 — Périmètre & mapping =========================
st.header("Étape 2 — Définir le périmètre depuis les XLS/XLSX")
st.write("On charge les fichiers extraits, on filtre **Custom Attribute5 Value** non vide (→ `Code DI`), et on sort `Classif DI` + méta.")

if st.button("📥 Charger & filtrer le périmètre"):
    try:
        xls_paths = sorted([*dest_dir.glob("*.xls"), *dest_dir.glob("*.xlsx")])
        if not xls_paths:
            st.warning(f"Aucun .xls/.xlsx trouvé dans {dest_dir}. Lance l’étape d’extraction d’abord.")
            st.stop()
        frames = []
        for p in xls_paths:
            st.write(f"Lecture: {p.name}")
            df = read_xls_smart(p)
            df = df.dropna(how="all")
            df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
            df = label_duplicate_columns(df)
            df["__source_file__"] = p.name
            frames.append(df)
        result, meta = build_perimeter(frames)

        st.info(f"Déduplication: {meta['dedup_before']} → {meta['dedup_after']} lignes (clés: {meta['dedup_keys']})")
        st.success(f"✅ Périmètre construit: {len(result)} lignes")
        st.dataframe(result.head(50))

        out_name = f"perimetre_IFTS_{file_tag}.xlsx"
        out_path = dest_dir / out_name
        with pd.ExcelWriter(out_path, engine="xlsxwriter") as wr:
            result.to_excel(wr, sheet_name="Perimetre", index=False)
        bio = BytesIO()
        with pd.ExcelWriter(bio, engine="xlsxwriter") as wr:
            result.to_excel(wr, sheet_name="Perimetre", index=False)
        st.download_button(
            label="⬇️ Télécharger le périmètre (Excel)",
            data=bio.getvalue(),
            file_name=out_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        st.exception(e)

with st.expander("Recommandations de modélisation (noms vs positions, legs)"):
    st.markdown(
        """
        **Colonnes: noms vs positions**  
        • Privilégier **les noms** (plus robustes aux insertions/suppressions).  
        • Gérer les **doublons** via suffixes `(Leg1)/(Leg2)`.  
        • Normaliser casse/espaces pour éviter les faux négatifs.

        **Colonnes répétées (Leg1/Leg2):**  
        • Détection et suffixage automatique.  
        • Pour passer en *tidy* plus tard: `pd.wide_to_long` / `stack`.
        """
    )

# ================== Étape 2.7 — Mapping YAML → template ======================
from openpyxl import load_workbook
from template_write import copy_template_to_dest

TEMPLATE_DEFAULT = Path(r"C:/Users/abenjelloun/OneDrive - Cooperactions/GAM-E-Risk Perf - RMP/1.PROD/4.REPORTINGS SPEC CLIENTS/1.Groupe - IFT (CB-JB)/IFT -template.xlsx")

YAML_DEFAULT = """# (identique à ta version, sauf commentaires)\n# sheet/header_row pilotent la feuille et la ligne d'en-têtes\nsheet: "IRS - INF – XCCY"\nheader_row: 6\n\n# Variables extraites par colonnes de source (lettres)\nvariables:\n  notional_leg1 :   { source_letter: "AX" }\n  notional_leg2 :  { source_letter: "BP" }\n  dirtyvalue_leg1:   { source_letter: "DL" }\n  cleanvalue_leg1:   { source_letter: "DM" }\n  accrued_leg1:   { source_letter: "DN" }\n  dirtyvalue_leg2:   { source_letter: "DS" }\n  cleanvalue_leg2:   { source_letter: "DT" }\n  accrued_leg2:   { source_letter: "DU" }\n  dirtyvalue_tot:   { source_letter: "DA" }\n  cleanvalue_tot:   { source_letter: "DB" }\n  accrued_tot:   { source_letter: "DC" }\n\ncolumns:\n  - target: "Code DI"\n    source: "Custom Attribute5 Value"\n  - target: "Classif DI"\n    source: "Class"\n  - target: "Class"\n    source: "Class"\n  - target: "External Id"\n    source: "External Id"\n  - target: "Counterparty"\n    source: "Counterparty"\n\n  # Leg 1\n  - target: "Leg Type"\n    target_occurrence: 1\n    source_letter: "AR"\n  - target: "Pay/Rec"\n    target_occurrence: 1\n    source_letter: "AS"\n  - target: "Index/Fixed Rate"\n    target_occurrence: 1\n    source_letter: "AT"\n  - target: "Spread (bp)"\n    target_occurrence: 1\n    source_letter: "AU"\n  - target: "Start Date"\n    target_occurrence: 1\n    source_letter: "AV"\n  - target: "End Date"\n    target_occurrence: 1\n    source_letter: "AW"\n  - target: "Notional"\n    target_occurrence: 1\n    source_letter: "AX"\n  - target: "Dirty Value"\n    target_occurrence: 1\n    source_letter: "DL"\n  - target: "Clean Value"\n    target_occurrence: 1\n    source_letter: "DM"\n  - target: "Accrued Interest"\n    target_occurrence: 1\n    source_letter: "DN"\n\n  # Leg 2\n  - target: "Leg Type"\n    target_occurrence: 2\n    source_letter: "BI"\n  - target: "Pay/Rec"\n    target_occurrence: 2\n    source_letter: "BJ"\n  - target: "Index/Fixed Rate"\n    target_occurrence: 2\n    source_letter: "BK"\n  - target: "Spread (bp)"\n    target_occurrence: 2\n    source_letter: "BL"\n  - target: "Start Date"\n    target_occurrence: 2\n    source_letter: "BM"\n  - target: "End Date"\n    target_occurrence: 2\n    source_letter: "BN"\n  - target: "Currency"\n    source: "Currency"\n  - target: "Notional"\n    target_occurrence: 2\n    source_letter: "BP"\n  - target: "Dirty Value"\n    target_occurrence: 2\n    source_letter: "DS"\n  - target: "Clean Value"\n    target_occurrence: 2\n    source_letter: "DT"\n  - target: "Accrued Interest"\n    target_occurrence: 2\n    source_letter: "DU"\n\n  # Total\n  - target: "Dirty Value"\n    target_occurrence: 3\n    source_letter: "DA"\n  - target: "Clean Value"\n    target_occurrence: 3\n    source_letter: "DB"\n  - target: "Accrued Interest"\n    target_occurrence: 3\n    source_letter: "DC"\n\ncomputed:\n  - target: "Dirty Value (%)"\n    target_occurrence: 1\n    expr: "dirtyvalue_leg1 / notional_leg1"\n  - target: "Clean Value (%)"\n    target_occurrence: 1\n    expr: "cleanvalue_leg1 / notional_leg1"\n  - target: "Accrued Interest (%)"\n    target_occurrence: 1\n    expr: "accrued_leg1 / notional_leg1"\n\n  - target: "Dirty Value (%)"\n    target_occurrence: 2\n    expr: "dirtyvalue_leg2 / notional_leg2"\n  - target: "Clean Value (%)"\n    target_occurrence: 2\n    expr: "cleanvalue_leg2 / notional_leg2"\n  - target: "Accrued Interest (%)"\n    target_occurrence: 2\n    expr: "accrued_leg2 / notional_leg2"\n\n  # Total% volontairement sur notional_leg1 (choix utilisateur)\n  - target: "Dirty Value (%)"\n    target_occurrence: 3\n    expr: "dirtyvalue_tot / notional_leg1"\n  - target: "Clean Value (%)"\n    target_occurrence: 3\n    expr: "cleanvalue_tot / notional_leg1"\n  - target: "Accrued Interest (%)"\n    target_occurrence: 3\n    expr: "accrued_tot / notional_leg1"\n"""

yaml_text = st.text_area("Mapping YAML", value=YAML_DEFAULT, height=360)

colYA, colYB = st.columns(2)
with colYA:
    do_preview_yaml = st.button("🧪 Prévisualiser (YAML)")
with colYB:
    do_integrate_yaml = st.button("🧩 Intégrer avec YAML → template")

if do_preview_yaml or do_integrate_yaml:
    try:
        cfg = load_cfg(yaml_text)
        # Charger data + ordre colonnes par fichier
        xls_paths = sorted([*dest_dir.glob("*.xls"), *dest_dir.glob("*.xlsx")])
        if not xls_paths:
            st.warning(f"Aucun .xls/.xlsx trouvé dans {dest_dir}.")
            st.stop()
        frames = []
        orders: dict[str, dict[str, str]] = {}  # fichier -> {LETTER -> colname}
        for p in xls_paths:
            df, letter_map = read_xls_with_positions(p)
            df = df.dropna(how="all")
            df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
            # label_duplicate_columns déjà appliqué dans read_xls_with_positions
            orders[p.name] = letter_map
            df["__source_file__"] = p.name
            frames.append(df)
        df_all = pd.concat(frames, ignore_index=True, sort=False)

        if do_preview_yaml:
            prev = preview_yaml_rows(df_all, orders, cfg, limit=200)
            st.success(f"Aperçu YAML: {len(prev)} lignes (tronqué à 200)")
            st.dataframe(prev)

        if do_integrate_yaml:
            out_xlsm = integrate_yaml_to_template(df_all, orders, cfg, TEMPLATE_DEFAULT, dest_dir, file_tag, mode, ifts_date)
            st.success(f"✅ Intégration YAML terminée → {out_xlsm}")
            with open(out_xlsm, "rb") as f:
                st.download_button(
                    label="⬇️ Télécharger le template rempli (.xlsx)",
                    data=f.read(),
                    file_name=out_xlsm.name,
                    mime="application/vnd.ms-excel.sheet.macroEnabled.12",
                )

            # Persiste le chemin pour usages ultérieurs (après rerun Streamlit)
            st.session_state["out_xlsm"] = str(out_xlsm)
            st.session_state["ifts_date"] = ifts_date
            # (La préparation d'email persistante est maintenant en dehors de ce bloc)
    except Exception as e:
        st.exception(e)


# ================= app.py (Étape 3 persistante) ==============================
# Ce bloc est *en dehors* du if do_preview_yaml/do_integrate_yaml, donc il reste
# visible après un rerun quand on clique sur un bouton.
if "out_xlsm" in st.session_state:
    st.divider()
    st.subheader("Étape 2.8 — Importer Sensis Bloomberg")

    expected_file = expected_sensis_name(st.session_state.get("ifts_date", ifts_date))
    st.caption(f"Fichier attendu dans le dossier prod : `{expected_file}`")

    if st.button("📊 Importer les données Sensis", key="import_sensis_btn"):
        try:
            out_xlsm_path = Path(st.session_state["out_xlsm"])
            sensis_dt = st.session_state.get("ifts_date", ifts_date)
            sensis_path = locate_sensis_file(dest_dir, sensis_dt)
            st.write(f"Fichier Sensis utilisé : **{sensis_path.name}**")
            sensis_table = load_sensis_table(sensis_path)
            updated, missing, rows_preview = apply_sensis_to_workbook(out_xlsm_path, sensis_table)
            st.success(f"{updated} ligne(s) mise(s) à jour dans le template.")
            if rows_preview:
                df_preview = pd.DataFrame(rows_preview)
                st.dataframe(df_preview)
            if missing:
                preview = ", ".join(missing[:10])
                if len(missing) > 10:
                    preview += " …"
                st.warning(f"Codes DI absents du Sensis : {preview}")
            mime = (
                "application/vnd.ms-excel.sheet.macroEnabled.12"
                if out_xlsm_path.suffix.lower() == ".xlsm"
                else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            with open(out_xlsm_path, "rb") as f:
                st.download_button(
                    label="⬇️ Télécharger le fichier mis à jour (Sensis)",
                    data=f.read(),
                    file_name=out_xlsm_path.name,
                    mime=mime,
                    key="download_sensis_updated",
                )
        except FileNotFoundError as e:
            st.error(str(e))
        except Exception as e:
            st.exception(e)

    st.divider()
    st.subheader("Étape 3 — Préparer l'email Outlook")

    TO_DEFAULT = "DOS SANTOS Nicolas <NDosSantos@groupama-am.fr>; VERGER Sebastien <SVerger@groupama-am.fr>; GAM MO - Collateral <Collateral-GAM@groupama-am.fr>"
    CC_DEFAULT = "BIDA MBOKE Jerry <JBidaMboke@groupama-am.fr>; GAM DQM-Pricing <DQM-Pricing@groupama-am.fr>; GAM MO - Derivatives <middleofficederivesOTC@groupama-am.fr>; GAM Liste Risques Performances <RisquesPerformances@groupama-am.fr>"

    # Destinataires pour la demande Trioptima
    TRIO_TO_DEFAULT = "GAM MO - Collateral <Collateral-GAM@groupama-am.fr>"
    TRIO_CC_DEFAULT = "GAM DQM-Pricing <DQM-Pricing@groupama-am.fr>; GAM Liste Risques Performances <RisquesPerformances@groupama-am.fr>; BIDA MBOKE Jerry <JBidaMboke@groupama-am.fr>; VERGER Sebastien <SVerger@groupama-am.fr>"

    # Destinataires pour le report collatéral
    COLL_TO_DEFAULT = "GAM MO - Collateral <Collateral-GAM@groupama-am.fr>"
    COLL_CC_DEFAULT = "BIDA MBOKE Jerry <JBidaMboke@groupama-am.fr>; GAM Liste Risques Performances <RisquesPerformances@groupama-am.fr>; CHAGROT Rene-Louis <RLChagrot@groupama-am.fr>"

    # Si on a demandé un reset au clic précédent, on supprime les clés AVANT de ré-instancier les widgets
    #if st.session_state.get("reset_mail_defaults", False):
    #    for k in ("mail_to_once", "mail_cc_once"):
    #        if k in st.session_state:
    #            del st.session_state[k]
    #    st.session_state["reset_mail_defaults"] = False


    send_mail = st.button("✉️ Préparer les brouillons des emails Outlook à envoyer", key="prepare_mail_btn")

    if send_mail:
        try:
            out_xlsm_path = Path(st.session_state["out_xlsm"])  # chemin persistant
            ifts_dt = st.session_state.get("ifts_date", ifts_date)
            final_name = build_ifts_filename(ifts_dt)
            attach_path = export_xlsx_copy(out_xlsm_path, final_name)
            #to_use = st.session_state.get("mail_to_once", TO_DEFAULT) or TO_DEFAULT
            #cc_use = st.session_state.get("mail_cc_once", CC_DEFAULT) or CC_DEFAULT
            to_use = TO_DEFAULT
            cc_use = CC_DEFAULT
            prepare_outlook_draft(attach_path, ifts_dt, to=to_use, cc=cc_use)
            # Deuxième mail simultané : demande du fichier Trioptima
            prepare_trioptima_request_mail(ifts_dt, to=TRIO_TO_DEFAULT, cc=TRIO_CC_DEFAULT)
            # Troisième mail simultané : report trimestriel de collatéral
            prepare_collateral_report_request_mail(ifts_dt, to=COLL_TO_DEFAULT, cc=COLL_CC_DEFAULT)
            st.success("Trois brouillons Outlook ouverts : 1) VALO IFT avec PJ ; 2) Demande Trioptima ; 3) Report collatéral.")
            # Demande de reset des champs *avant* le prochain rendu
            #st.session_state["reset_mail_defaults"] = True
            # Relance (compatibilité selon la version de Streamlit)
            _rerun = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
            if callable(_rerun):
                _rerun()
        except Exception as e:
            st.exception(e)