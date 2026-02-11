from pathlib import Path
import duckdb
import pandas as pd
import streamlit as st

# -----------------------------
# CONFIGURATION DE LA PAGE
# -----------------------------
st.set_page_config(
    page_title="Dashboard Netflix",
    layout="wide"
)

st.title("Dashboard Netflix - CSV intégré au projet")

# -----------------------------
# 1) LIRE LE CSV LOCAL DANS data/
# -----------------------------
# BASE_DIR = dossier où se trouve ce fichier netflix_app.py
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "Netflix Datasets Evaluation MS Excel.csv"

st.sidebar.header("Données")
st.sidebar.write(f"Fichier utilisé : `{DATA_PATH}`")

# Lecture du CSV (sans upload utilisateur)
df_raw = pd.read_csv(DATA_PATH)

st.sidebar.success("CSV chargé depuis le projet.")
st.write("Aperçu des données brutes :")
st.dataframe(df_raw.head(), use_container_width=True)

# -----------------------------
# 2) CRÉER LA BASE DUCKDB EN MÉMOIRE
# -----------------------------
con = duckdb.connect(database=":memory:")

# On supprime les doublons éventuels sur show_id
df = df_raw.drop_duplicates(subset=["show_id"])

# Création de la table netflix dans DuckDB
con.execute("DROP TABLE IF EXISTS netflix")
con.register("netflix_df", df)
con.execute("CREATE TABLE netflix AS SELECT * FROM netflix_df")

TABLE_NAME = "netflix"

# -----------------------------
# 3) LISTES POUR LES FILTRES
# -----------------------------
types = con.execute(
    f"""
    SELECT DISTINCT type
    FROM {TABLE_NAME}
    WHERE type IS NOT NULL
    ORDER BY type
    """
).df()["type"].tolist()

years = con.execute(
    f"""
    SELECT DISTINCT release_year
    FROM {TABLE_NAME}
    WHERE release_year IS NOT NULL
    ORDER BY release_year
    """
).df()["release_year"].tolist()

countries = con.execute(
    f"""
    SELECT DISTINCT split_part(country, ',', 1) AS main_country
    FROM {TABLE_NAME}
    WHERE country IS NOT NULL AND country <> ''
    ORDER BY main_country
    """
).df()["main_country"].tolist()

st.sidebar.header("Filtres")

selected_types = st.sidebar.multiselect(
    "Type",
    options=types,
    default=types
)

selected_years = st.sidebar.multiselect(
    "Années de sortie",
    options=years,
    default=years
)

selected_countries = st.sidebar.multiselect(
    "Pays (premier pays listé)",
    options=countries,
    default=[]
)

# -----------------------------
# 4) CONSTRUIRE LA CLAUSE WHERE
# -----------------------------
where_clauses = []

if selected_types:
    types_str = ",".join([f"'{t}'" for t in selected_types])
    where_clauses.append(f"type IN ({types_str})")

if selected_years:
    years_str = ",".join(str(y) for y in selected_years)
    where_clauses.append(f"release_year IN ({years_str})")

if selected_countries:
    countries_str = ",".join([f"'{c}'" for c in selected_countries])
    where_clauses.append(f"split_part(country, ',', 1) IN ({countries_str})")

where_sql = ""
if where_clauses:
    where_sql = "WHERE " + " AND ".join(where_clauses)

# -----------------------------
# 5) RÉCUPÉRER LES DONNÉES FILTRÉES
# -----------------------------
query_base = f"""
    SELECT
        show_id,
        type,
        title,
        split_part(country, ',', 1) AS main_country,
        release_year,
        rating,
        duration,
        listed_in AS genres
    FROM {TABLE_NAME}
    {where_sql}
"""

df_netflix = con.execute(query_base).df()

if df_netflix.empty:
    st.warning("Aucun titre ne correspond aux filtres sélectionnés.")
    st.stop()

# -----------------------------
# 6) TRAITER LA DURÉE (en minutes)
# -----------------------------
df_netflix[["duration_value", "duration_unit"]] = df_netflix["duration"].str.split(
    " ", n=1, expand=True
)
df_netflix["duration_value"] = pd.to_numeric(
    df_netflix["duration_value"], errors="coerce"
)

def duration_to_minutes(row):
    if pd.isna(row["duration_value"]):
        return None
    if isinstance(row["duration_unit"], str) and row["duration_unit"].startswith("min"):
        return row["duration_value"]
    else:
        # Approximation pour les saisons de séries
        return row["duration_value"] * 10 * 45

df_netflix["duration_min"] = df_netflix.apply(duration_to_minutes, axis=1)

# -----------------------------
# 7) KPI PRINCIPAUX
# -----------------------------
nb_titles = len(df_netflix)
nb_movies = df_netflix[df_netflix["type"] == "Movie"].shape[0]
nb_tvshows = df_netflix[df_netflix["type"] == "TV Show"].shape[0]
nb_countries = df_netflix["main_country"].nunique()
duree_moy = df_netflix["duration_min"].dropna().mean()

st.markdown("## KPI principaux")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Nombre de titres", f"{nb_titles:,}".replace(",", " "))
with col2:
    st.metric("Nombre de films", f"{nb_movies:,}".replace(",", " "))
with col3:
    st.metric("Nombre de séries", f"{nb_tvshows:,}".replace(",", " "))
with col4:
    if pd.isna(duree_moy):
        st.metric("Durée moyenne", "N/A")
    else:
        st.metric("Durée moyenne", f"{duree_moy:.0f} min")

st.markdown("---")

# -----------------------------
# 8) GRAPHE 1 : TITRES PAR ANNÉE & TYPE
# -----------------------------
st.subheader("Nombre de titres par année et par type")

df_year_type = (
    df_netflix
    .groupby(["release_year", "type"], as_index=False)
    .agg(nb_titles=("show_id", "count"))
    .sort_values("release_year")
)

if not df_year_type.empty:
    chart_data = df_year_type.pivot(
        index="release_year",
        columns="type",
        values="nb_titles"
    )
    st.line_chart(chart_data)
else:
    st.write("Pas assez de données pour ce graphique.")

# -----------------------------
# 9) GRAPHE 2 : FILMS VS SÉRIES
# -----------------------------
st.subheader("Répartition Films vs Séries")

df_type = (
    df_netflix
    .groupby("type", as_index=False)
    .agg(nb_titles=("show_id", "count"))
    .sort_values("nb_titles", ascending=False)
)

st.bar_chart(df_type.set_index("type")["nb_titles"])

# -----------------------------
# 10) GRAPHE 3 : TOP 10 PAYS
# -----------------------------
st.subheader("Top 10 pays par nombre de titres")

df_country = (
    df_netflix
    .groupby("main_country", as_index=False)
    .agg(nb_titles=("show_id", "count"))
    .sort_values("nb_titles", ascending=False)
    .head(10)
)

st.bar_chart(df_country.set_index("main_country")["nb_titles"])

# -----------------------------
# 11) TABLE DÉTAILLÉE
# -----------------------------
st.markdown("---")
with st.expander("Voir la table détaillée des titres filtrés"):
    st.dataframe(
        df_netflix[
            [
                "title",
                "type",
                "main_country",
                "release_year",
                "rating",
                "duration",
                "genres",
            ]
        ].sort_values("release_year", ascending=False),
        use_container_width=True,
    )

