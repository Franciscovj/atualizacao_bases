import os
import pandas as pd

def clear_data(df0: pd.DataFrame, season_id: int, league_name: str, pasta: str, ano: str) -> str:
    """
    Filtra os dados por temporada, ajusta o nome da liga e salva em um arquivo CSV.

    Parâmetros:
        df0 (pd.DataFrame): DataFrame com todos os dados.
        season_id (int): ID da temporada a ser filtrada.
        league_name (str): Nome da liga (usado no nome do arquivo e na coluna).
        pasta (str): Caminho da pasta onde o arquivo será salvo.
        ano (str): Ano usado no nome do arquivo.

    Retorna:
        str: Caminho completo do arquivo CSV salvo.
    """

    # Validação
    if not isinstance(df0, pd.DataFrame):
        raise TypeError("O parâmetro 'df0' deve ser um pandas.DataFrame.")
    if "season_id" not in df0.columns:
        raise ValueError("A coluna 'season_id' não existe no DataFrame.")

    # Filtra a liga
    df_liga = df0[df0["season_id"] == season_id].copy()

    # 🔹 Atualiza a coluna 'league_name' com o mesmo nome usado no arquivo
    if "league_name" in df_liga.columns:
        df_liga["league_name"] = league_name
    else:
        # Se não existir, cria a coluna
        df_liga["league_name"] = league_name

    df_liga["Temporada"] = ano     

    # 🔻 Remove colunas indesejadas antes de salvar
    drop_cols = ["league_country_id", "season_id"]
    df_liga = df_liga.drop(columns=[c for c in drop_cols if c in df_liga.columns])

    # Cria diretório se não existir
    os.makedirs(pasta, exist_ok=True)

    # Normaliza nome seguro
    safe_league_name = str(league_name).strip().replace(" ", "_").replace("/", "-")

    # Caminho do arquivo
    file_path = os.path.join(pasta, f"{safe_league_name}_{ano}.csv")

    # Salva o CSV
    df_liga.to_csv(file_path, index=False, encoding="utf-8-sig")

    # Logs
    print(f"✅ Arquivo salvo com sucesso: {file_path}")
    print(f"📊 Registros exportados: {len(df_liga)}")
    print(f"🏷️ Nome da liga aplicado na coluna: {league_name}")

    return file_path
