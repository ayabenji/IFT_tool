from __future__ import annotations

from datetime import date
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st
from trioptima_import import (
    aggregate_trioptima,
    apply_trioptima_to_workbook,
    build_trioptima_mapping,
    expected_trioptima_prefix,
    load_trioptima_table,
    locate_trioptima_file,
)
from excel_read import label_duplicate_columns, read_xls_smart, read_xls_with_positions
from io_zip import ensure_dir, extract_xls_from_zip
from mail_outlook import (
    build_ifts_filename,
    export_xlsx_copy,
    prepare_collateral_report_request_mail,
    prepare_outlook_draft,
    prepare_trioptima_request_mail,
)
from perimeter import build_perimeter
from sensis_import import (
    apply_sensis_to_workbook,
    load_sensis_table,
    locate_sensis_file,
)
from yaml_apply import integrate_yaml_to_template, load_cfg, preview_yaml_rows


TEMPLATE_DEFAULT = Path(
    r"C:/Users/abenjelloun/OneDrive - Cooperactions/GAM-E-Risk Perf - RMP/1.PROD/4.REPORTINGS SPEC CLIENTS/1.Groupe - IFT (CB-JB)/IFT -template.xlsx"
)

YAML_DEFAULT = """# (identique à ta version, sauf commentaires)
# sheet/header_row pilotent la feuille et la ligne d'en-têtes
sheet: "IRS - INF – XCCY"
header_row: 6

# Variables extraites par colonnes de source (lettres)
variables:
  notional_leg1:   { source_letter: "AX" }
  notional_leg2 :  { source_letter: "BP" }
  dirtyvalue_leg1:   { source_letter: "DL" }
  cleanvalue_leg1:   { source_letter: "DM" }
  accrued_leg1:   { source_letter: "DN" }
  dirtyvalue_leg2:   { source_letter: "DS" }
  cleanvalue_leg2:   { source_letter: "DT" }
  accrued_leg2:   { source_letter: "DU" }
  dirtyvalue_tot:   { source_letter: "DA" }
  cleanvalue_tot:   { source_letter: "DB" }
  accrued_tot:   { source_letter: "DC" }

columns:
  - target: "Code DI"
    source: "Custom Attribute5 Value"
  - target: "Classif DI"
    source: "Class"
  - target: "Class"
    source: "Class"
  - target: "External Id"
    source: "External Id"
  - target: "Counterparty"
    source: "Counterparty"

  # Leg 1
  - target: "Leg Type"
    target_occurrence: 1
    source_letter: "AR"
  - target: "Pay/Rec"
    target_occurrence: 1
    source_letter: "AS"
  - target: "Index/Fixed Rate"
    target_occurrence: 1
    source_letter: "AT"
  - target: "Spread (bp)"
    target_occurrence: 1
    source_letter: "AU"
  - target: "Start Date"
    target_occurrence: 1
    source_letter: "AV"
  - target: "End Date"
    target_occurrence: 1
    source_letter: "AW"
  - target: "Notional"
    target_occurrence: 1
    source_letter: "AX"
  - target: "Dirty Value"
    target_occurrence: 1
    source_letter: "DL"
  - target: "Clean Value"
    target_occurrence: 1
    source_letter: "DM"
  - target: "Accrued Interest"
    target_occurrence: 1
    source_letter: "DN"

  # Leg 2
  - target: "Leg Type"
    target_occurrence: 2
    source_letter: "BI"
  - target: "Pay/Rec"
    target_occurrence: 2
    source_letter: "BJ"
  - target: "Index/Fixed Rate"
    target_occurrence: 2
    source_letter: "BK"
  - target: "Spread (bp)"
    target_occurrence: 2
    source_letter: "BL"
  - target: "Start Date"
    target_occurrence: 2
    source_letter: "BM"
  - target: "End Date"
    target_occurrence: 2
    source_letter: "BN"
  - target: "Currency"
    source: "Currency"
  - target: "Notional"
    target_occurrence: 2
    source_letter: "BP"
  - target: "Dirty Value"
    target_occurrence: 2
    source_letter: "DS"
  - target: "Clean Value"
    target_occurrence: 2
    source_letter: "DT"
  - target: "Accrued Interest"
    target_occurrence: 2
    source_letter: "DU"

  # Total
  - target: "Dirty Value"
    target_occurrence: 3
    source_letter: "DA"
  - target: "Clean Value"
    target_occurrence: 3
    source_letter: "DB"
  - target: "Accrued Interest"
    target_occurrence: 3
    source_letter: "DC"

computed:
  - target: "Dirty Value (%)"
    target_occurrence: 1
    expr: "dirtyvalue_leg1 / notional_leg1"
  - target: "Clean Value (%)"
    target_occurrence: 1
    expr: "cleanvalue_leg1 / notional_leg1"
  - target: "Accrued Interest (%)"
    target_occurrence: 1
    expr: "accrued_leg1 / notional_leg1"

  - target: "Dirty Value (%)"
    target_occurrence: 2
    expr: "dirtyvalue_leg2 / notional_leg2"
  - target: "Clean Value (%)"
    target_occurrence: 2
    expr: "cleanvalue_leg2 / notional_leg2"
  - target: "Accrued Interest (%)"
    target_occurrence: 2
    expr: "accrued_leg2 / notional_leg2"

  # Total% volontairement sur notional_leg1 (choix utilisateur)
  - target: "Dirty Value (%)"
    target_occurrence: 3
    expr: "dirtyvalue_tot / notional_leg1"
  - target: "Clean Value (%)"
    target_occurrence: 3
    expr: "cleanvalue_tot / notional_leg1"
  - target: "Accrued Interest (%)"
    target_occurrence: 3
    expr: "accrued_tot / notional_leg1"
"""


def render_workflow_tab(
    src_dir: Path,
    dest_dir: Path,
    patterns: list[str],
    file_tag: str,
    mode: str,
    ifts_date: date,
) -> None:
    with st.expander("Chemins (lecture seule)"):
        st.code(f"Source: {src_dir}\nDestination: {dest_dir}")

    if st.button("🔎 Rechercher & extraire"):
        try:
            if not src_dir.exists():
                st.error(f"Dossier source introuvable: {src_dir}")
                st.stop()
            ensure_dir(dest_dir)
            found: list[Path] = []
            for pat in patterns:
                matches = list(src_dir.glob(pat))
                if matches:
                    st.write(f"Pattern **{pat}** → {len(matches)} fichier(s) trouvé(s)")
                else:
                    st.warning(f"Pattern **{pat}** → aucun fichier trouvé")
                found.extend(matches)
            if not found:
                st.stop()

            total_extracted: list[Path] = []
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
        except Exception as exc:
            st.exception(exc)

    st.header("Étape 2 — Définir le périmètre depuis les XLS/XLSX")
    st.write(
        "On charge les fichiers extraits, on filtre **Custom Attribute5 Value** non vide (→ `Code DI`), et on sort `Classif DI` + méta."
    )

    if st.button("📥 Charger & filtrer le périmètre"):
        try:
            xls_paths = sorted([*dest_dir.glob("*.xls")])
            if not xls_paths:
                st.warning(
                    f"Aucun .xls/.xlsx trouvé dans {dest_dir}. Lance l’étape d’extraction d’abord."
                )
                st.stop()
            frames = []
            for path in xls_paths:
                st.write(f"Lecture: {path.name}")
                df = read_xls_smart(path)
                df = df.dropna(how="all")
                df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
                df = label_duplicate_columns(df)
                df["__source_file__"] = path.name
                frames.append(df)
            result, meta = build_perimeter(frames)

            st.info(
                f"Déduplication: {meta['dedup_before']} → {meta['dedup_after']} lignes (clés: {meta['dedup_keys']})"
            )
            st.success(f"✅ Périmètre construit: {len(result)} lignes")
            st.dataframe(result.head(50))

            out_name = f"perimetre_IFTS_{file_tag}.xlsx"
            out_path = dest_dir / out_name
            with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
                result.to_excel(writer, sheet_name="Perimetre", index=False)

        except Exception as exc:
            st.exception(exc)

    yaml_text = YAML_DEFAULT


    do_create_ift_file = st.button("Générer le fichier des IFTs")

    if  do_create_ift_file:
        try:
            cfg = load_cfg(yaml_text)
            xls_paths = sorted([*dest_dir.glob("*.xls"), *dest_dir.glob("*.xlsx")])
            if not xls_paths:
                st.warning(f"Aucun .xls/.xlsx trouvé dans {dest_dir}.")
                st.stop()
            frames = []
            orders: dict[str, dict[str, str]] = {}
            for path in xls_paths:
                df, letter_map = read_xls_with_positions(path)
                df = df.dropna(how="all")
                df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
                orders[path.name] = letter_map
                df["__source_file__"] = path.name
                frames.append(df)
            df_all = pd.concat(frames, ignore_index=True, sort=False)

            if do_create_ift_file:
                out_xlsm = integrate_yaml_to_template(
                    df_all, orders, cfg, TEMPLATE_DEFAULT, dest_dir, file_tag, mode, ifts_date
                )
                st.success(f"Fichier généré → {out_xlsm}")

                st.session_state["out_xlsm"] = str(out_xlsm)
                st.session_state["ifts_date"] = ifts_date
        except Exception as exc:
            st.exception(exc)

    if "out_xlsm" in st.session_state:
        st.divider()
        st.subheader("Étape 2.8 — Importer Sensis Bloomberg")

        st.caption(f"Fichier attendu dans le dossier prod : `sensis_IR_*.xls`")

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
            except FileNotFoundError as exc:
                st.error(str(exc))
            except Exception as exc:
                st.exception(exc)

        st.divider()
        st.subheader("Étape 2.9 — Importer TriOptima")

        triopt_prefix = expected_trioptima_prefix(st.session_state.get("ifts_date", ifts_date))
        st.caption(f"Fichier attendu dans le dossier prod : `{triopt_prefix}*.csv`")

        if st.button("📈 Importer les données TriOptima", key="import_trioptima_btn"):
            try:
                out_xlsm_path = Path(st.session_state["out_xlsm"])
                triopt_dt = st.session_state.get("ifts_date", ifts_date)
                triopt_path = locate_trioptima_file(dest_dir, triopt_dt)
                st.write(f"Fichier TriOptima utilisé : **{triopt_path.name}**")

                triopt_df = load_trioptima_table(triopt_path)
                if triopt_df.empty:
                    st.warning("Aucune ligne TriOptima avec FREE_TEXT_2 renseigné n'a été trouvée.")
                    st.stop()

                aggregated = aggregate_trioptima(triopt_df)
                st.dataframe(aggregated, use_container_width=True)

                mapping = build_trioptima_mapping(aggregated)
                updated, missing_codes, preview_rows, unused_codes = apply_trioptima_to_workbook(
                    out_xlsm_path, mapping
                )
                st.success(f"{updated} ligne(s) mise(s) à jour dans le template.")

                if preview_rows:
                    st.dataframe(pd.DataFrame(preview_rows), use_container_width=True)

                if missing_codes:
                    preview = ", ".join(missing_codes[:10])
                    if len(missing_codes) > 10:
                        preview += " …"
                    st.warning(f"Codes DI absents du TriOptima : {preview}")

                if unused_codes:
                    preview = ", ".join(unused_codes[:10])
                    if len(unused_codes) > 10:
                        preview += " …"
                    st.info(f"Codes TriOptima non utilisés dans le template : {preview}")

                mime = (
                    "application/vnd.ms-excel.sheet.macroEnabled.12"
                    if out_xlsm_path.suffix.lower() == ".xlsm"
                    else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            except FileNotFoundError as exc:
                st.error(str(exc))
            except Exception as exc:
                st.exception(exc)        

        st.divider()
        st.subheader("Étape 3 — Préparer l'email Outlook")

        to_default = (
            "DOS SANTOS Nicolas <NDosSantos@groupama-am.fr>; VERGER Sebastien <SVerger@groupama-am.fr>; "
            "GAM MO - Collateral <Collateral-GAM@groupama-am.fr>"
        )
        cc_default = (
            "BIDA MBOKE Jerry <JBidaMboke@groupama-am.fr>; GAM DQM-Pricing <DQM-Pricing@groupama-am.fr>; "
            "GAM MO - Derivatives <middleofficederivesOTC@groupama-am.fr>; "
            "GAM Liste Risques Performances <RisquesPerformances@groupama-am.fr>"
        )
        trio_to_default = "GAM MO - Collateral <Collateral-GAM@groupama-am.fr>"
        trio_cc_default = (
            "GAM DQM-Pricing <DQM-Pricing@groupama-am.fr>; GAM Liste Risques Performances <RisquesPerformances@groupama-am.fr>;"
            "BIDA MBOKE Jerry <JBidaMboke@groupama-am.fr>; VERGER Sebastien <SVerger@groupama-am.fr>"
        )
        coll_to_default = "GAM MO - Collateral <Collateral-GAM@groupama-am.fr>"
        coll_cc_default = (
            "BIDA MBOKE Jerry <JBidaMboke@groupama-am.fr>; GAM Liste Risques Performances <RisquesPerformances@groupama-am.fr>;"
            "CHAGROT Rene-Louis <RLChagrot@groupama-am.fr>"
        )

        if st.button("✉️ Préparer les brouillons des emails Outlook à envoyer", key="prepare_mail_btn"):
            try:
                out_xlsm_path = Path(st.session_state["out_xlsm"])
                ifts_dt = st.session_state.get("ifts_date", ifts_date)
                final_name = build_ifts_filename(ifts_dt)
                attach_path = export_xlsx_copy(out_xlsm_path, final_name)
                prepare_outlook_draft(attach_path, ifts_dt, to=to_default, cc=cc_default)
                prepare_trioptima_request_mail(ifts_dt, to=trio_to_default, cc=trio_cc_default)
                prepare_collateral_report_request_mail(ifts_dt, to=coll_to_default, cc=coll_cc_default)
                st.success(
                    "Trois brouillons Outlook ouverts : 1) VALO IFT avec PJ ; 2) Demande Trioptima ; 3) Report collatéral."
                )
                rerun = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
                if callable(rerun):
                    rerun()
            except Exception as exc:
                st.exception(exc)