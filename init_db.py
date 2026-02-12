import duckdb
import os

# 1. Dossiers de travail
DATA_CSV_DIR = 'data_csv' # Là où sont tes CSV WhatsApp
DATA_DB_DIR = 'data'      # Là où on va créer la base de données
# On nomme la base "ma_base_globale" pour bien montrer qu'elle contient TOUT
DB_FILE = os.path.join(DATA_DB_DIR, 'ma_base_globale.db')

# 2. Liste de tes 3 fichiers différents
# À gauche : le nom de la table dans DuckDB / À droite : le nom exact du fichier CSV
fichiers_csv = {
    "table_netflix": "Netflix Datasets Evaluation MS Excel.csv",
    "table_artists": "artists.csv",
    "table_tracks": "tracks.csv"
}

def init_db():
    # Création du dossier data s'il n'existe pas
    if not os.path.exists(DATA_DB_DIR):
        os.makedirs(DATA_DB_DIR)

    # Connexion à la base unique (qui va contenir les 3 tables)
    con = duckdb.connect(DB_FILE)
    
    print(f"🚀 Début de l'importation vers {DB_FILE}...\n")

    for table_name, file_name in fichiers_csv.items():
        chemin_complet_csv = os.path.join(DATA_CSV_DIR, file_name)
        
        if os.path.exists(chemin_complet_csv):
            print(f"📦 Importation : {file_name} -> Table '{table_name}'")
            # Cette commande crée une table spécifique pour chaque CSV
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{chemin_complet_csv}')")
        else:
            print(f"❌ Fichier manquant dans {DATA_CSV_DIR} : {file_name}")

    con.close()
    print("\n✅ Base DuckDB prête avec vos différentes tables !")

if __name__ == "__main__":
    init_db()