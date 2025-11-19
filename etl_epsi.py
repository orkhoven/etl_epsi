import streamlit as st
import os
import base64
import requests
import datetime
import re

# ---------------------------
# CONFIG
# ---------------------------

st.set_page_config(
    page_title="TP Intégration de données – AdventureWorks",
    layout="wide"
)

# À ADAPTER SELON TON REPO
GITHUB_REPO = "orkhoven/etl_epsi"  # ex: "orkhoven/nom-du-repo"
GITHUB_BRANCH = "main"
GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN", None)      # à ajouter dans .streamlit/secrets.toml
SUBMISSIONS_DIR = "submissions"                          # dossier dans le repo

# 4 CSV avec les noms proches des fichiers originaux AdventureWorks
DATA_FILES = [
    ("Ventes 2020 (Sales)", "data/AdventureWorks Sales Data 2020.csv"),
    ("Clients (Customer Lookup)", "data/AdventureWorks Customer Lookup.csv"),
    ("Produits (Product Lookup)", "data/AdventureWorks Product Lookup.csv"),
    ("Territoires (Territory Lookup)", "data/AdventureWorks Territory Lookup.csv"),
]


# ---------------------------
# OUTILS
# ---------------------------

def slugify(text: str, default: str = "etudiant") -> str:
    """
    Simplifie le nom de l'étudiant pour l'utiliser dans un chemin de fichier.
    """
    if not text:
        return default
    text = text.strip().lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_]+", "", text)
    return text or default


def upload_file_to_github(file_bytes: bytes, dest_path: str, token: str, repo: str, branch: str = "main") -> requests.Response:
    """
    Envoie un fichier dans un repo GitHub via l'API.
    Crée toujours un nouveau fichier (nom unique) pour éviter la gestion des SHA.
    """
    if token is None:
        raise RuntimeError("Aucun token GitHub trouvé dans st.secrets['GITHUB_TOKEN'].")

    url = f"https://api.github.com/repos/{repo}/contents/{dest_path}"
    b64_content = base64.b64encode(file_bytes).decode("utf-8")

    data = {
        "message": f"Ajout dépôt étudiant : {dest_path}",
        "content": b64_content,
        "branch": branch,
    }

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
    }

    response = requests.put(url, headers=headers, json=data)
    return response


# ---------------------------
# UI
# ---------------------------

st.title("TP Intégration de données – AdventureWorks")
st.markdown(
    """
Ce TP correspond à la **Partie 1 – Intégration de données en Python** du module  
**TRDE703 – Intégration des données (Mastère Expert en Cybersécurité)**.

L’objectif est de construire un **mini pipeline ETL en Python** avant de passer aux outils ETL (Talend / Pentaho / SSIS) dans la séance suivante.
"""
)

tab_story, tab_instructions, tab_data, tab_submit = st.tabs(
    ["📖 Contexte & histoire", "🧪 Consignes & livrables", "📥 Jeux de données", "📤 Dépôt sur GitHub"]
)

# ---------------------------
# Onglet 1 – Contexte & histoire
# ---------------------------

with tab_story:
    st.header("Contexte : l’entreprise AdventureWorks")

    st.markdown(
        """
AdventureWorks est un **fabricant et vendeur mondial de vélos, pièces et accessoires**.  
L’entreprise vend :

- via des **magasins physiques**,  
- via une **plateforme de vente en ligne**.

Pour fonctionner, AdventureWorks s’appuie sur plusieurs systèmes :

- un **ERP** pour les commandes et la logistique,  
- un **CRM** pour les clients,  
- un **système catalogue produits**,  
- un **système de gestion des territoires commerciaux**.

Chacun de ces systèmes exporte des données dans des fichiers séparés (CSV).  
La direction veut maintenant **centraliser** ces données pour suivre :

- la performance des ventes,  
- le comportement client,  
- la performance par territoire,  
- la rentabilité par catégorie de produits.

Vous êtes **data engineer junior**. Votre mission pour cette séance :  
construire, en Python, un **flux d’intégration de données** à partir de plusieurs fichiers AdventureWorks,  
et produire une table propre, prête pour l’analyse.
"""
    )

    st.subheader("Objectif pédagogique")
    st.markdown(
        """
À la fin de ce TP, vous devez être capable de :

- **Extraire** des données issues de plusieurs fichiers (sources distinctes),  
- **Transformer** ces données (nettoyage, typage, jointures, colonnes dérivées),  
- **Charger** le résultat dans un fichier unique (CSV ou base SQLite).

Ce que vous faites ici en Python sera **rejoué avec un outil ETL** lors de la séance suivante.
"""
    )

# ---------------------------
# Onglet 2 – Consignes & livrables
# ---------------------------

with tab_instructions:
    st.header("Consignes pour les étudiant·e·s")

    st.markdown(
        """
### 1. Sources de données

Vous disposez de **4 fichiers CSV** représentant des systèmes différents :

1. **AdventureWorks Sales Data 2020.csv**  
   → données de ventes (ERP) pour l’année 2020  

2. **AdventureWorks Customer Lookup.csv**  
   → référentiel clients (CRM)  

3. **AdventureWorks Product Lookup.csv**  
   → référentiel produits (catalogue)  

4. **AdventureWorks Territory Lookup.csv**  
   → référentiel territoires commerciaux  

Tous ces fichiers proviennent du même univers AdventureWorks, mais **ne sont pas intégrés**.

---

### 2. Votre mission – Mini pipeline ETL en Python

Vous devez construire, dans un script Python ou un notebook, un flux qui réalise :

#### E – Extract (Extraction)

- Charger les 4 fichiers depuis le disque avec `pandas.read_csv`.
- Vérifier rapidement la structure de chaque table (`head()`, `info()`, `describe()` pour les variables numériques).

#### T – Transform (Transformation)

- Vérifier et convertir les types :
  - dates (ex : date de commande s’il y en a),  
  - variables numériques (ex : prix, quantités, remises).
- Traiter les **valeurs manquantes** (choisir : suppression, imputation simple… et **justifier** dans le rapport).
- Traiter les **données incohérentes** (quantité ≤ 0, remise négative ou trop élevée, etc.).
- Supprimer les **doublons** pertinents.
- Réaliser les **jointures** pour construire une table intégrée :
  - `Sales` + `Customer Lookup`,  
  - `Sales` + `Product Lookup`,  
  - `Sales` + `Territory Lookup`.
- Créer quelques **colonnes dérivées** (adapter aux colonnes disponibles) :
  - année de commande (`OrderYear` ou équivalent),  
  - chiffre d’affaires de ligne (`LineTotal`),  
  - éventuellement marge si les colonnes le permettent.

#### L – Load (Chargement)

- Sauvegarder la table finale dans un **fichier unique** :
  - format recommandé : `clean_adventureworks_sales_2020.csv`
- Optionnel (bonus) : charger dans une base **SQLite** (`to_sql`).

---

### 3. KPIs à produire dans le notebook / script

À partir de votre table intégrée :

1. Chiffre d’affaires et quantité vendue par **année** (ou mois) et par **catégorie de produit** (selon les colonnes du Product Lookup).  
2. Top 10 des **clients** par chiffre d’affaires total.  
3. Chiffre d’affaires par **territoire** (ou pays / région selon les colonnes du Territory Lookup).

Les KPIs peuvent être affichés avec `groupby` et `agg` dans des DataFrames formatés.

---

### 4. Livrables attendus

Vous devez déposer via l’interface (onglet **“📤 Dépôt sur GitHub”**) :

1. **Votre code** :
   - soit un **notebook** (`.ipynb`),  
   - soit un **script Python** (`.py`).
2. Un **court rapport** (max 1 page, `.pdf`, `.md`, `.txt` ou `.docx`) répondant aux points suivants :
   - principaux **problèmes de qualité de données** identifiés,  
   - **règles de nettoyage** appliquées (et pourquoi),  
   - schéma simple de votre pipeline (sources → Python → table finale),  
   - 2–3 **indicateurs clés** que vous jugez exploitables.

Le dépôt sur GitHub est géré automatiquement par cette application.
"""
    )

# ---------------------------
# Onglet 3 – Jeux de données
# ---------------------------

with tab_data:
    st.header("Jeux de données pour le TP")

    st.markdown(
        """
Les fichiers utilisés dans ce TP doivent être placés **dans le dossier `data/` de l’application Streamlit**  
avec **les noms suivants** :

- `AdventureWorks Sales Data 2020.csv`  
- `AdventureWorks Customer Lookup.csv`  
- `AdventureWorks Product Lookup.csv`  
- `AdventureWorks Territory Lookup.csv`

Pour chaque jeu de données ci-dessous, si le fichier existe côté serveur, vous verrez un bouton de téléchargement.
Sinon, un message indiquera au formateur qu’il doit ajouter le fichier correspondant.
"""
    )

    for label, path in DATA_FILES:
        st.subheader(label)
        if os.path.exists(path):
            with open(path, "rb") as f:
                data_bytes = f.read()
            st.download_button(
                label=f"Télécharger : {os.path.basename(path)}",
                data=data_bytes,
                file_name=os.path.basename(path),
                mime="text/csv"
            )
            st.caption(f"Fichier trouvé : `{path}`")
        else:
            st.warning(
                f"Fichier introuvable : `{path}`. "
                "Le formateur doit ajouter ce fichier dans le répertoire de l’application."
            )

    st.info(
        "Remarque : les données AdventureWorks peuvent être récupérées depuis Kaggle ou un dépôt GitHub, "
        "puis copiées / renommées pour correspondre exactement aux fichiers ci-dessus."
    )

# ---------------------------
# Onglet 4 – Dépôt sur GitHub
# ---------------------------

with tab_submit:
    st.header("Dépôt de votre travail sur GitHub")

    st.markdown(
        """
Remplissez les informations ci-dessous et uploadez vos fichiers.  
L’application créera automatiquement une entrée dans le dépôt GitHub du formateur.
"""
    )

    with st.form("submission_form", clear_on_submit=False):
        col1, col2 = st.columns(2)
        with col1:
            student_name = st.text_input("Nom / Prénom", placeholder="Ex : Dupont Alice")
            student_group = st.text_input("Groupe / Promo", placeholder="Ex : ECYB I1")
        with col2:
            student_email = st.text_input("E-mail (optionnel)", placeholder="Ex : prenom.nom@exemple.com")
            comment = st.text_area("Commentaire (optionnel)", placeholder="Notes pour le formateur...")

        st.markdown("### Fichiers à déposer")

        code_file = st.file_uploader(
            "Notebook ou script Python",
            type=["ipynb", "py"],
            help="Fichier principal contenant votre code (obligatoire)."
        )

        report_file = st.file_uploader(
            "Rapport (1 page max)",
            type=["pdf", "md", "txt", "docx"],
            help="Court rapport expliquant vos choix de nettoyage et d’intégration (optionnel mais recommandé)."
        )

        confirm = st.checkbox("Je confirme que ces fichiers constituent ma soumission pour ce TP.")

        submitted = st.form_submit_button("📤 Envoyer sur GitHub")

    if submitted:
        if not confirm:
            st.error("Vous devez cocher la case de confirmation avant d’envoyer.")
        elif not code_file:
            st.error("Le fichier de code (notebook ou script Python) est obligatoire.")
        elif GITHUB_TOKEN is None:
            st.error("Aucun token GitHub n’est configuré. Contacter le formateur.")
        else:
            try:
                now = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                student_slug = slugify(student_name)

                # Dossier étudiant
                base_dir = f"{SUBMISSIONS_DIR}/{student_slug}_{now}"

                results = []

                # 1) Fichier de code
                code_bytes = code_file.read()
                code_ext = os.path.splitext(code_file.name)[1]
                code_dest = f"{base_dir}/code{code_ext}"
                resp_code = upload_file_to_github(
                    file_bytes=code_bytes,
                    dest_path=code_dest,
                    token=GITHUB_TOKEN,
                    repo=GITHUB_REPO,
                    branch=GITHUB_BRANCH
                )
                results.append(("code", resp_code))

                # 2) Rapport (optionnel)
                if report_file is not None:
                    report_bytes = report_file.read()
                    report_ext = os.path.splitext(report_file.name)[1]
                    report_dest = f"{base_dir}/rapport{report_ext}"
                    resp_report = upload_file_to_github(
                        file_bytes=report_bytes,
                        dest_path=report_dest,
                        token=GITHUB_TOKEN,
                        repo=GITHUB_REPO,
                        branch=GITHUB_BRANCH
                    )
                    results.append(("rapport", resp_report))

                # 3) Métadonnées (nom, email, groupe, commentaire)
                meta_content = f"""Nom complet : {student_name}
Groupe / Promo : {student_group}
E-mail : {student_email}
Commentaire : {comment}
Date (UTC) : {datetime.datetime.utcnow().isoformat()}
"""
                meta_bytes = meta_content.encode("utf-8")
                meta_dest = f"{base_dir}/meta.txt"
                resp_meta = upload_file_to_github(
                    file_bytes=meta_bytes,
                    dest_path=meta_dest,
                    token=GITHUB_TOKEN,
                    repo=GITHUB_REPO,
                    branch=GITHUB_BRANCH
                )
                results.append(("meta", resp_meta))

                # Vérification des réponses GitHub
                ok = all(r.status_code in (200, 201) for _, r in results)
                if ok:
                    st.success("Votre dépôt a bien été envoyé sur GitHub. ✅")
                    for label, r in results:
                        st.write(f"- {label} → statut GitHub : {r.status_code}")
                else:
                    st.error("Une erreur est survenue lors de l’envoi sur GitHub.")
                    for label, r in results:
                        st.write(f"- {label} → statut GitHub : {r.status_code} / réponse : {r.text}")

            except Exception as e:
                st.error(f"Erreur lors de l’envoi sur GitHub : {e}")
