import streamlit as st
import os
import base64
import requests
from datetime import datetime, timezone
import re

st.set_page_config(
    page_title="TP Intégration de données – AdventureWorks",
    layout="wide"
)

GITHUB_REPO = "orkhoven/etl_epsi"
GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"]
SUBMISSIONS_DIR = "submissions"

DATA_FILES = [
    ("Ventes 2020 (Sales)", "data/AdventureWorks Sales Data 2020.csv"),
    ("Clients (Customer Lookup)", "data/AdventureWorks Customer Lookup.csv"),
    ("Produits (Product Lookup)", "data/AdventureWorks Product Lookup.csv"),
    ("Territoires (Territory Lookup)", "data/AdventureWorks Territory Lookup.csv"),
]


def slugify(text: str, default: str = "etudiant") -> str:
    if not text:
        return default
    text = text.strip().lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_]+", "", text)
    return text or default


def upload_to_github_bytes(file_bytes: bytes, dest_path: str, repo: str, token: str, commit_msg: str | None = None) -> int:
    api_url = f"https://api.github.com/repos/{repo}/contents/{dest_path}"
    content_b64 = base64.b64encode(file_bytes).decode("utf-8")

    headers = {"Authorization": f"token {token}"}
    data = {"message": commit_msg or f"Add {dest_path}", "content": content_b64}

    resp = requests.put(api_url, headers=headers, json=data)
    if resp.status_code not in (200, 201):
        st.error(f"Erreur GitHub ({resp.status_code}) pour {dest_path} : {resp.text}")
    else:
        st.info(f"Fichier envoyé : {dest_path}")
    return resp.status_code


st.title("TP Intégration de données – AdventureWorks")
st.markdown(
    """
Ce TP correspond à la **Partie 1 – Intégration de données en Python** du module  
**TRDE703 – Intégration des données (Mastère Expert en Cybersécurité)**.

Objectif : construire un mini pipeline ETL en Python avant l’utilisation d’outils ETL.
"""
)

tab_story, tab_instructions, tab_data, tab_submit = st.tabs(
    ["Contexte & histoire", "Consignes & livrables", "Jeux de données", "Dépôt sur GitHub"]
)

with tab_story:
    st.header("Contexte : AdventureWorks")

    st.markdown(
        """
AdventureWorks est un fabricant et vendeur mondial de vélos et accessoires.  
L’entreprise utilise plusieurs systèmes distincts : ERP, CRM, catalogue produits, territoires commerciaux.

La mission consiste à intégrer ces données pour obtenir une table analytique unique.
"""
    )

    st.subheader("Objectifs pédagogiques")
    st.markdown(
        """
- Extraction de fichiers CSV  
- Transformation : typage, nettoyage, jointures, colonnes dérivées  
- Chargement dans un fichier unique  
- Préparation de KPIs
"""
    )


with tab_instructions:
    st.header("Consignes")

    st.markdown(
        """
### 1. Sources de données  
Quatre fichiers CSV provenant de systèmes différents.

### 2. Mission ETL en Python
- Extraction : chargement CSV  
- Transformation : types, valeurs manquantes, incohérences, doublons, jointures, colonnes dérivées  
- Chargement : fichier final unique

### 3. KPIs
- CA et quantités par période et catégorie  
- Top 10 clients  
- CA par territoire

### 4. Livrables
- Notebook `.ipynb` ou script `.py`  
- Rapport (1 page) : qualité des données, règles de nettoyage, schéma du pipeline, KPIs
"""
    )


with tab_data:
    st.header("Jeux de données")

    st.markdown(
        """
Les fichiers doivent se trouver dans le dossier `data/` avec les noms suivants :
"""
    )

    for label, path in DATA_FILES:
        st.subheader(label)
        if os.path.exists(path):
            with open(path, "rb") as f:
                data_bytes = f.read()
            st.download_button(
                label=os.path.basename(path),
                data=data_bytes,
                file_name=os.path.basename(path),
                mime="text/csv"
            )
            st.caption(f"Fichier disponible : {path}")
        else:
            st.warning(f"Fichier introuvable : {path}")


with tab_submit:
    st.header("Dépôt sur GitHub")

    with st.form("submission_form", clear_on_submit=False):
        col1, col2 = st.columns(2)
        with col1:
            student_name = st.text_input("Nom / Prénom")
            student_group = st.text_input("Groupe / Promo")
        with col2:
            student_email = st.text_input("E-mail (optionnel)")
            comment = st.text_area("Commentaire (optionnel)")

        st.markdown("Fichiers à déposer")

        code_file = st.file_uploader(
            "Notebook ou script Python",
            type=["ipynb", "py"]
        )

        report_file = st.file_uploader(
            "Rapport (optionnel)",
            type=["pdf", "md", "txt", "docx"]
        )

        confirm = st.checkbox("Je confirme ma soumission.")

        submitted = st.form_submit_button("Envoyer sur GitHub")

    if submitted:
        if not confirm:
            st.error("Veuillez confirmer la soumission.")
        elif not code_file:
            st.error("Le fichier de code est obligatoire.")
        else:
            try:
                now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                student_slug = slugify(student_name)
                base_dir = f"{SUBMISSIONS_DIR}/{student_slug}_{now}"

                results = []

                code_bytes = code_file.read()
                code_ext = os.path.splitext(code_file.name)[1]
                code_dest = f"{base_dir}/code{code_ext}"
                status_code_code = upload_to_github_bytes(
                    code_bytes, code_dest, GITHUB_REPO, GITHUB_TOKEN, f"TP ETL - code - {student_name}"
                )
                results.append(("code", status_code_code))

                if report_file is not None:
                    report_bytes = report_file.read()
                    report_ext = os.path.splitext(report_file.name)[1]
                    report_dest = f"{base_dir}/rapport{report_ext}"
                    status_code_report = upload_to_github_bytes(
                        report_bytes, report_dest, GITHUB_REPO, GITHUB_TOKEN, f"TP ETL - rapport - {student_name}"
                    )
                    results.append(("rapport", status_code_report))

                meta_content = f"""Nom : {student_name}
Groupe : {student_group}
Email : {student_email}
Commentaire : {comment}
Date UTC : {datetime.now(timezone.utc).isoformat()}
"""
                meta_bytes = meta_content.encode("utf-8")
                meta_dest = f"{base_dir}/meta.txt"
                status_code_meta = upload_to_github_bytes(
                    meta_bytes, meta_dest, GITHUB_REPO, GITHUB_TOKEN, f"TP ETL - meta - {student_name}"
                )
                results.append(("meta", status_code_meta))

                ok = all(code in (200, 201) for _, code in results)

                if ok:
                    st.success("Dépôt envoyé sur GitHub.")
                else:
                    st.error("Une erreur est survenue lors du dépôt.")
                    for label, code in results:
                        st.write(f"- {label} → statut GitHub : {code}")

            except Exception as e:
                st.error(f"Erreur lors de l’envoi : {e}")
