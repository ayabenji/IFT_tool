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
    apply_bndfwd_to_workbook,
    filter_bndfwd_rows,
    
)
from collateral_compare import (
    aggregate_template_mtm,
    build_collateral_comparison,
    find_collateral_report,
    load_collateral_summary,
    parse_alias_mapping,
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
DEFAULT_COLLATERAL_CP_ALIASES = """\
# Alias contreparties → valeur canonique
SGCIB = SOCIETE GENERALE
SOCIETE GENERALE = SOCIETE GENERALE
BAMLS = BOFA SECURITIES
BOFA = BOFA SECURITIES
BOFA SECURITIES = BOFA SECURITIES
RBS = NATWEST MARKETS
NATWEST = NATWEST MARKETS
NATWEST MARKETS = NATWEST MARKETS
BARCLAYS = BARCLAYS BANK
BARCLAYS BANK = BARCLAYS BANK
GSOH = GOLDMAN SACHS
GOLDMAN SACHS = GOLDMAN SACHS
HSBCFR = HSBC CONTINENTAL EUROPE
HSBC = HSBC CONTINENTAL EUROPE
HSBC CONTINENTAL EUROPE = HSBC CONTINENTAL EUROPE
CREDIT SUISSE = CREDIT SUISSE
NOMURA = NOMURA FINANCIAL
NOMURA FINANCIAL = NOMURA FINANCIAL
MORGAN STANLEY = MORGAN STANLEY
BNPP = BNP PARIBAS
BNP PARIBAS = BNP PARIBAS
JPMSE = JP MORGAN SE
JP MORGAN = JP MORGAN SE
JP MORGAN SE = JP MORGAN SE
JPM = JP MORGAN SE
NATIXIS = NATIXIS
CA = CREDIT AGRICOLE
CEP = CREDIT AGRICOLE
CEPS = CREDIT AGRICOLE
CACIB = CREDIT AGRICOLE
CREDIT AGRICOLE = CREDIT AGRICOLE
""".strip()


DEFAULT_COLLATERAL_TYPOLOGY_ALIASES = """\
# Typologie → valeur canonique
Real Rate Swap = XpressInstrument
XpressInstrument = XpressInstrument
CMS Swap = IR swap, fixed/float
IR swap, fixed/float = IR swap, fixed/float
Cross Currency Swap = Cross currency, fixed/fixed
Cross currency, fixed/fixed = Cross currency, fixed/fixed
Forward = Forward
""".strip()


def render_workflow_tab(
    src_dir: Path,
    dest_dir: Path,
    patterns: list[str],
    file_tag: str,
    mode: str,
    ifts_date: date,
) -> None:
    with st.expander("Chemins qui sont utilisés pour récupérer les DEX SD"):
        st.code(f"Source: {src_dir}\nDestination: {dest_dir}")
    st.warning(f"Renseigner bien la date des IFTs et le mode -> date de valo - 19/09/2025 pour le fast de ce mois.")
    st.write("Dans les dex SD, c'est le fichier du jour ouvré suivant. Le code les récupère directement. Appuyer sur le bouton.")


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

    st.header("Définir le périmètre depuis les fichiers SD")
    st.write(
        "On charge les fichiers extraits, on filtre par rapport à **Custom Attribute5 Value** non vide (→ `Code DI`), et on génère le fichier des IFTS qui sera rempli dans les étapes suivantes."
    )

    if st.button("Filtrer le périmètre et générer le fichier des IFTs"):
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

            yaml_text = YAML_DEFAULT
            cfg = load_cfg(yaml_text)
            xls_paths = sorted([*dest_dir.glob("*.xls")])
            if not xls_paths:
                st.warning(f"Aucun .xls trouvé dans {dest_dir}.")
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
            out_xlsm = integrate_yaml_to_template(
                    df_all, orders, cfg, TEMPLATE_DEFAULT, dest_dir, ifts_date, mode, ifts_date
                )
            st.success(f"Fichier généré → {out_xlsm}")

            st.session_state["out_xlsm"] = str(out_xlsm)
            st.session_state["ifts_date"] = ifts_date

            st.write("Le fichier des IFTs avec les prix de Gam étant près, on envoie le mail à Sebastier Verger pour qu'il vérifie que les données SD sont en ligne avec les données dans SCD qui rentrent dans le rapport EMIR envoyé par DQM. " )
            st.write("En appuyant sur le bouton : Préparer les brouillons des emails à envoyer : \n" 
            "-> 3 mails seront préparés :\n " 
            "- Le premier qui demandera à Sebastier Verger de vérifier les données SD.\n" \
            "- Le deuxième qui demandera au MO (Sylvain) d'envoyer le fichier trioptima du jour des IFTs dès que possible.\n" \
            "- Le troisième qui demandera au MO d'envoyer le report de Collatéral close du jours des IFTs.")


        except Exception as exc:
            st.exception(exc)

    
        
    if "out_xlsm" in st.session_state:

        st.divider()
        st.subheader(" Préparer l'email Outlook")

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
                
        st.divider()
        st.subheader(" Enrichir le fichier des IFTs avec les sensis SD ")
        st.write("Lancer dans File Upload and Report Activation dans la page Blotter de SD, un report activation. On sélectionne \n" \
                "- Product : IR\n" \
                "- Report Type : MTM IPA \n" \
                "- Valuation Date : la date des IFTs \n" \
                "- Books : Cross Currency Swap , Swap CMS et Asset Swap Inflation \n" \
                "- Template Name : RD_IFT Report IT\n" \
                "- File Prefix : sensis_ pour le code puisse départager le fichier des sensis des fichiers DEX")
        st.write("Télécharger le fichier une fois prêt et le mettre dans le dossier prod dans le mode choisi (fast ou closed)")
        if st.button("📊 Importer les données Sensis", key="import_sensis_btn"):
            try:
                
                

                st.caption(f"Fichier attendu dans le dossier prod : `sensis_IR_*.xls`")

                out_xlsm_path = Path(st.session_state["out_xlsm"])
                sensis_dt = st.session_state.get("ifts_date", ifts_date)
                sensis_path = locate_sensis_file(dest_dir, sensis_dt)
                st.write(f"Fichier Sensis utilisé : **{sensis_path.name}**")
                sensis_table = load_sensis_table(sensis_path)
                updated, missing, rows_preview = apply_sensis_to_workbook(out_xlsm_path, sensis_table,ifts_date=sensis_dt)
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
        st.subheader("Importer TriOptima - Rajouter les prix contreparties pour les swaps, et les données des Bonds Forwards")
        st.write("Récupérer le fichier envoyé par le MO (Sylvain) daté du jour des IFTs et le mettre dans le dossier prod ")
        triopt_prefix = expected_trioptima_prefix(st.session_state.get("ifts_date", ifts_date))
        st.caption(f"Fichier attendu dans le dossier prod : `{triopt_prefix}*.csv`")

        if st.button("📈 Importer les données TriOptima", key="import_trioptima_btn"):
            try:
                out_xlsm_path = Path(st.session_state["out_xlsm"])
                triopt_dt = st.session_state.get("ifts_date", ifts_date)
                triopt_path = locate_trioptima_file(dest_dir, triopt_dt)
                st.write(f"Fichier TriOptima utilisé : **{triopt_path.name}**")

                triopt_df = load_trioptima_table(triopt_path)

                missing_bndfwd_cols: list[str] = triopt_df.attrs.get(
                    "missing_bndfwd_columns", []
                )

                aggregated = aggregate_trioptima(triopt_df)
                if aggregated.empty:
                    st.warning("Aucune ligne TriOptima avec FREE_TEXT_2 renseigné n'a été trouvée.")
                mapping = build_trioptima_mapping(aggregated)

                if mapping:
                    (
                        updated,
                        missing_codes,
                        preview_rows,
                        unused_codes,
                    ) = apply_trioptima_to_workbook(out_xlsm_path, mapping)
                    st.success(f"{updated} ligne(s) mise(s) à jour dans le template.")


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
                else:
                    st.info("Aucun Code DI TriOptima à injecter dans le template.")
                
                if missing_bndfwd_cols:
                    cols = ", ".join(missing_bndfwd_cols)
                    st.warning(
                        "Colonnes TriOptima manquantes pour la feuille BND FWD : "
                        f"{cols}"
                    )
                else:
                    try:
                        bndfwd_rows = filter_bndfwd_rows(triopt_df)
                        
                    except KeyError as exc:
                        st.warning(str(exc))
                    else:
                        bnd_updated, bnd_missing, bnd_preview, bnd_alerts = apply_bndfwd_to_workbook(
                            out_xlsm_path, bndfwd_rows
                        )
                        if bnd_updated:
                            st.success(
                                f"{bnd_updated} ligne(s) BND FWD mise(s) à jour dans le template."
                            )
                        else:
                            st.info(
                                "Feuille 'BND FWD' nettoyée (aucune ligne BNDFWD avec Book 601/602/603)."
                            )

                        if not bndfwd_rows.empty:
                            preview_cols = [
                                col
                                for col in [
                                    "FREE_TEXT_1",
                                    "BOOK",
                                    "CP",
                                    "NOTIONAL",
                                    "MTM_VALUE",
                                    "MTM_DIFF",
                                    "MTM_CONTREPARTIE",
                                ]
                                if col in bndfwd_rows.columns
                            ]

                        if bnd_preview:
                            st.dataframe(pd.DataFrame(bnd_preview), use_container_width=True)

                        if bnd_alerts:
                            preview = ", ".join(bnd_alerts[:10])
                            if len(bnd_alerts) > 10:
                                preview += " …"
                            st.warning(f"Seuil 0,5 % dépassé pour : {preview}")

                        if bnd_missing:
                            preview = "\n- ".join(bnd_missing[:5])
                            if preview:
                                preview = "- " + preview
                            if len(bnd_missing) > 5:
                                preview += "\n…"
                            st.warning(f"Données manquantes BND FWD :\n{preview}")

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
        st.subheader("Étape 2.10 — Contrôle du report collatéral")

        with st.expander("📊 Comparer le template avec le report collatéral", expanded=False):
            st.caption(
                "Recherche dans le dossier destination un fichier `*Report Collatéral.xlsx` puis compare les MtM."
            )

            st.session_state.setdefault(
                "collateral_cp_aliases", DEFAULT_COLLATERAL_CP_ALIASES
            )
            st.session_state.setdefault(
                "collateral_typ_aliases", DEFAULT_COLLATERAL_TYPOLOGY_ALIASES
            )

            cp_alias_text = st.text_area(
                "Alias contreparties",
                key="collateral_cp_aliases",
                help="Format : alias = valeur canonique (un par ligne).",
            )
            typ_alias_text = st.text_area(
                "Alias typologies",
                key="collateral_typ_aliases",
                help="Format : alias = valeur canonique (un par ligne).",
            )

            counterparty_aliases = parse_alias_mapping(cp_alias_text)
            typology_aliases = parse_alias_mapping(typ_alias_text)

            if st.button(
                "🔍 Calculer les écarts collatéral",
                key="compare_collateral_btn",
            ):
                try:
                    out_xlsm_path = Path(st.session_state["out_xlsm"])
                except KeyError:
                    st.warning("Génère d'abord le fichier IFT pour lancer la comparaison.")
                else:
                    try:
                        collateral_path = find_collateral_report(dest_dir)
                    except FileNotFoundError as exc:
                        st.error(str(exc))
                    else:
                        st.write(
                            f"Report collatéral utilisé : **{collateral_path.name}**"
                        )
                        try:
                            collateral_df = load_collateral_summary(collateral_path)
                        except Exception as exc:
                            st.exception(exc)
                        else:
                            try:
                                template_df = aggregate_template_mtm(out_xlsm_path)
                            except Exception as exc:
                                st.exception(exc)
                            else:
                                with st.expander(
                                    "Données sources chargées", expanded=False
                                ):
                                    st.markdown("**Synthèse report collatéral**")
                                    st.dataframe(
                                        collateral_df, use_container_width=True
                                    )
                                    st.markdown("**Agrégats template IFT**")
                                    st.dataframe(
                                        template_df, use_container_width=True
                                    )

                                comparison = build_collateral_comparison(
                                    template_df,
                                    collateral_df,
                                    counterparty_aliases=counterparty_aliases,
                                    typology_aliases=typology_aliases,
                                )

                                if comparison.empty:
                                    st.info(
                                        "Aucune ligne comparable entre le template et le report collatéral."
                                    )
                                else:
                                    max_abs = (
                                        comparison[
                                            [
                                                "Ecart MtM Gam",
                                                "Ecart MtM Counterparty",
                                            ]
                                        ]
                                        .abs()
                                        .to_numpy()
                                    ).max()
                                    if max_abs < 1e-6:
                                        st.success(
                                            "Les montants agrégés concordent avec le report collatéral."
                                        )
                                    else:
                                        st.warning(
                                            f"Écart absolu maximal : {max_abs:,.2f}"
                                        )

                                    st.dataframe(
                                        comparison, use_container_width=True
                                    )

                                    csv_bytes = (
                                        comparison.to_csv(index=False).encode("utf-8-sig")
                                    )
                                    st.download_button(
                                        "⬇️ Exporter les écarts (CSV)",
                                        data=csv_bytes,
                                        file_name="ecarts_collateral.csv",
                                        mime="text/csv",
                                    )

                                    excel_buffer = BytesIO()
                                    comparison.to_excel(excel_buffer, index=False)
                                    st.download_button(
                                        "⬇️ Exporter les écarts (Excel)",
                                        data=excel_buffer.getvalue(),
                                        file_name="ecarts_collateral.xlsx",
                                        mime=(
                                            "application/vnd.openxmlformats-officedocument."
                                            "spreadsheetml.sheet"
                                        ),
                                    )    