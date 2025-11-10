import pandas as pd
import numpy as np
from pymongo import MongoClient
from config import MONGO_URI, MONGO_COLLECTION
from modules.league_export import clear_data
from modules.teams_export import export_all_leagues
from modules.utils import cols, numeric_cols, ligas_2025, liga_ids_2025
import os

print("🚀 Iniciando script...")

# Pastas e arquivos estáticos (ano 2022)
PES_DIR = "season_2025_pes"
PASTA_LIGAS = os.path.join(PES_DIR, "ligas")
PASTA_STATS = os.path.join(PES_DIR, "stats_teams_2025")
PASTA_LIGAS_FULL = os.path.join(PES_DIR, "ligas_full")
PASTA_STATS_FULL = os.path.join(PES_DIR, "stats_teams_full_2025")
TEAMS_CSV_PATH = os.path.join("output", "teams_season_stats_2025.csv")
FULL_STATS_FILENAME = "teams_season_stats_full_2025.csv"

# Usa diretamente as listas de 2022
ligas = ligas_2025
liga_ids = liga_ids_2025

# ==========================
# Conexão segura com MongoDB
# ==========================
print("🔹 Conectando ao MongoDB...")
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.server_info()  # testa a conexão
    db = client.get_database()
    collection = db[MONGO_COLLECTION]
    print("✅ Conexão com MongoDB estabelecida!")
except Exception as e:
    raise SystemExit(f"❌ Erro na conexão com MongoDB: {e}")

# ==========================
# Carregar dados
# ==========================
print("🔹 Carregando dados do MongoDB...")
data = list(collection.find())
if not data:
    raise SystemExit("❌ Nenhum dado encontrado no MongoDB!")

df = pd.DataFrame(data)
print(f"✅ Dados carregados: {len(df)} linhas")

pd.set_option('display.max_columns', None)

# ==========================
# Limpeza e preparação
# ==========================
print("🔹 Removendo duplicatas pela coluna 'id'...")
df = df.drop_duplicates(subset='id', keep='first').reset_index(drop=True)
print(f"✅ Dados após remover duplicatas: {len(df)} linhas")

# Carrega o CSV de stats dos times (2024)
df_teams = pd.read_csv(TEAMS_CSV_PATH)

# Criar colunas que não existem
for col in cols:
    if col not in df.columns:
        df[col] = np.nan
print(f"✅ Colunas faltantes criadas.")

# Seleciona colunas desejadas
df0 = df[cols].copy()
print(f"🔹 DataFrame preparado: {len(df0)} linhas x {len(df0.columns)} colunas")

# ==========================
# Converte colunas para inteiro
# ==========================
for col in ["home_goals", "away_goals"]:
    if col in df0.columns:
        df0[col] = pd.to_numeric(df0[col], errors="coerce").fillna(0).astype(int)
print("✅ Colunas de gols convertidas para inteiro.")

# ==========================
# Loop pelas ligas (2024)
# ==========================
arquivos = []
os.makedirs(PASTA_LIGAS, exist_ok=True)

for season_id, nome, ano in ligas:
    print(f"\n===============================")
    print(f"⚡ Processando liga: {nome} ({ano}), season_id={season_id}")
    arquivo_clear = clear_data(df0, season_id, nome, PASTA_LIGAS, ano)
    arquivos.append(arquivo_clear)
    print(f"✅ clear_data processado: {arquivo_clear}")

# ==========================
# Log final
# ==========================
print("\n===============================")
print("✅ Todos os arquivos processados via clear_data():")
print("\n".join(arquivos))
print("🚀 Script finalizado com sucesso!")

# Pasta de saída para CSVs por liga
os.makedirs(PASTA_STATS, exist_ok=True)

# Exporta um CSV por liga/temporada (2024)
arquivos_teams = export_all_leagues(df_teams, ligas, PASTA_STATS)

print("\n✅ Todos os arquivos gerados:")
print("\n".join(arquivos_teams))

# Filtra apenas ids selecionados (2024)
df_filtrado = df0[df0['season_id'].isin(liga_ids)].copy()

df_filtrado = df_filtrado.drop(columns=['league_country_id', 'season_id'], errors='ignore')
# Salvar CSV filtrado em pasta 'ligas_full' (2024)
os.makedirs(PASTA_LIGAS_FULL, exist_ok=True)
df_filtrado.to_csv(os.path.join(PASTA_LIGAS_FULL, "ligas_full_2025.csv"), index=False)

print(f"✅ CSV salvo com {len(df_filtrado)} linhas (apenas ligas selecionadas).")

# Consolidado full (removendo colunas id) em pasta 'stats_teams_full_2024'
os.makedirs(PASTA_STATS_FULL, exist_ok=True)
file_path = os.path.join(PASTA_STATS_FULL, FULL_STATS_FILENAME)
drop_cols = ['team_id', 'season_id']
df_out = df_teams.drop(columns=[c for c in drop_cols if c in df_teams.columns])
df_out.to_csv(file_path, index=False, encoding="utf-8-sig")

print(f"✅ CSV consolidado salvo: {file_path} (linhas={len(df_teams)})")

