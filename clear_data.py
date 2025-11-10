import pandas as pd
import numpy as np
from pymongo import MongoClient
from config import MONGO_URI, MONGO_COLLECTION
from modules.league_export import clear_data
from modules.utils import cols, ligas_2025, numeric_cols, liga_ids_2025
from modules.mean_leagues import gerar_media, mean_leagues_odds
import os

print("🚀 Iniciando script...")

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

# Criar colunas que não existem
for col in cols:
    if col not in df.columns:
        df[col] = np.nan
print(f"✅ Colunas faltantes criadas.")

# Seleciona colunas desejadas
df0 = df[cols].copy()
df01 = df0.copy()
print(f"🔹 DataFrame preparado: {len(df01)} linhas x {len(df01.columns)} colunas")

# ==========================
# Cálculos gerais
# ==========================
print("🔹 Calculando médias gerais e probabilidades...")
mean_leagues_odds(df01)
print("✅ Cálculos gerais concluídos.")

# ==========================
# Converte colunas para inteiro
# ==========================
for col in ["home_goals", "away_goals"]:
    if col in df0.columns:
        df0[col] = pd.to_numeric(df0[col], errors="coerce").fillna(0).astype(int)
print("✅ Colunas de gols convertidas para inteiro.")

# ==========================
# Loop pelas ligas
# ==========================
arquivos = []
# Lista para acumular os DataFrames finais por liga (home+away) para consolidar depois
dfs_por_liga = []

for season_id, nome, ano in ligas_2025:
    print(f"\n===============================")
    print(f"⚡ Processando liga: {nome} ({ano}), season_id={season_id}")
    
    # Pastas
    pasta_liga = f"season_2025/ligas"
    os.makedirs(pasta_liga, exist_ok=True)
    
    pasta_media = f"season_2025/stats_teams_2025"
    os.makedirs(pasta_media, exist_ok=True)
    
    # Filtra dados
    df_filtrado = df01[df01["season_id"] == season_id].copy()
    if df_filtrado.empty:
        print(f"⚠️ Nenhum dado para a liga {nome}. Pulando...")
        continue
    print(f"🔹 {len(df_filtrado)} linhas filtradas para a liga {nome}")
    
    # Clear data
    arquivo_clear = clear_data(df_filtrado, season_id, nome, pasta_liga, ano)
    arquivos.append(arquivo_clear)
    print(f"✅ clear_data processado: {arquivo_clear}")
    
    # Gerar médias
    print("🔹 Gerando médias home/away...")
    df_home_stats = gerar_media(df_filtrado, "home", "home_team", numeric_cols)
    df_away_stats = gerar_media(df_filtrado, "away", "away_team", numeric_cols)
    print(f"✅ Médias geradas: home({len(df_home_stats)} linhas), away({len(df_away_stats)} linhas)")
    
    # Adicionar liga e ano
    df_home_stats["name_league"] = nome
    df_home_stats["ano"] = ano
    df_away_stats["name_league"] = nome
    df_away_stats["ano"] = ano
    
    # Merge home + away
    print("🔹 Realizando merge home + away...")
    df_total = pd.merge(df_home_stats, df_away_stats, on=["team", "name_league", "ano"], how="outer")
    print(f"✅ Merge concluído: {len(df_total)} linhas")
    
    # Substituir espaços por "_" no nome do arquivo
    nome_arquivo = nome.replace(" ", "_")
    arquivo = f"{pasta_media}/media_times_{nome_arquivo}_{ano}.csv"
    
    # Salvar CSV final por liga
    df_total.to_csv(arquivo, index=False)
    print(f"✅ Arquivo final salvo após merge: {arquivo}")
    
    # Acumular DataFrame final desta liga para consolidação posterior
    dfs_por_liga.append(df_total)

# ==========================
# Log final
# ==========================
print("\n===============================")
print("✅ Todos os arquivos processados via clear_data():")
print("\n".join(arquivos))
print("🚀 Script finalizado com sucesso!")


# Consolidação: gerar arquivo único com todas as ligas (sem alterar lógica existente)
if dfs_por_liga:
    # Concatena todos os DataFrames finais (home+away) das ligas
    df_consolidado = pd.concat(dfs_por_liga, ignore_index=True)
    
    # Define pasta e arquivo de saída (pasta solicitada: season_2025_pes/stats_teams_full_2025)
    pasta_consolidado = os.path.join("season_2025", "stats_teams_full_2025")
    os.makedirs(pasta_consolidado, exist_ok=True)
    file_path = os.path.join(pasta_consolidado, "teams_season_stats_full_2025.csv")
    
    # Salva o consolidado em CSV
    df_consolidado.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"✅ Arquivo consolidado salvo: {file_path} ({len(df_consolidado)} linhas)")
else:
    print("⚠️ Nenhuma média por liga encontrada para consolidar.")


df_filtrado = df0[df0['season_id'].isin(liga_ids_2025)].copy()
# Remove as colunas indesejadas
df_filtrado = df_filtrado.drop(columns=['league_country_id', 'season_id'], errors='ignore')

os.makedirs('season_2025/ligas_full', exist_ok=True)
df_filtrado.to_csv('season_2025/ligas_full/ligas_full_2025.csv', index=False)
print(f"✅ CSV salvo com {len(df_filtrado)} linhas (apenas ligas selecionadas).")


