import pandas as pd 
from modules.utils import cols, ligas_2025, numeric_cols, liga_ids_2025
from modules.mean_leagues import gerar_media, mean_leagues_odds
import os 

# Usa diretamente as listas de 2022
ligas = ligas_2025
liga_ids = liga_ids_2025




df = pd.read_csv('output/fixtures_data.csv')

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




dfs_por_liga = []
arquivos = []

for season_id, nome, ano in ligas:
    print(f"\n===============================")
    print(f"⚡ Processando liga: {nome} ({ano}), season_id={season_id}")
    
    
    pasta_media = f"season_2025_pes/stats_teams_mean_simple_2025"
    os.makedirs(pasta_media, exist_ok=True)
    
    # Filtra dados
    df_filtrado = df01[df01["season_id"] == season_id].copy()
    if df_filtrado.empty:
        print(f"⚠️ Nenhum dado para a liga {nome}. Pulando...")
        continue
    print(f"🔹 {len(df_filtrado)} linhas filtradas para a liga {nome}")

    
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
    arquivo = f"{pasta_media}/mean_simple_times_{nome_arquivo}_{ano}.csv"
    
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
    pasta_consolidado = os.path.join("season_2025_pes", "stats_teams_mean_simple_full_2025")
    os.makedirs(pasta_consolidado, exist_ok=True)
    file_path = os.path.join(pasta_consolidado, "teams_season_mean_simple_full_2025.csv")
    
    # Salva o consolidado em CSV
    df_consolidado.to_csv(file_path, index=False, encoding="utf-8-sig")
    print(f"✅ Arquivo consolidado salvo: {file_path} ({len(df_consolidado)} linhas)")
else:
    print("⚠️ Nenhuma média por liga encontrada para consolidar.")

