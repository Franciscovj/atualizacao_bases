"""
Utilitários para exportar dados de times (season stats) em CSV por liga/temporada.

Uso em notebook (clear_data.ipynb):

    import pandas as pd
    from modules.teams_export import clear_data_teams

    df_teams = pd.read_csv('season_2025/stats_teams_2025/teams_season_stats_2025.csv')
    out = clear_data_teams(df_teams, 25673, 'Spain La Liga 2', 'season_2025/ligas', '2025-2026')
    print(out)
"""
from __future__ import annotations
import os
import re
import pandas as pd
from typing import List


def _slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[\s/\\]+", "_", text)        # espaços e barras -> _
    text = re.sub(r"[^a-z0-9_\-]", "", text)      # remove chars não-alfanuméricos
    text = re.sub(r"_+", "_", text).strip("_")     # colapsa múltiplos _
    return text or "liga"


def clear_data_teams(
    df_teams: pd.DataFrame,
    season_id: int,
    nome_liga: str,
    pasta_base: str,
    ano_label: str,
) -> str:
    """
    Filtra o DataFrame de teams por season_id e salva em CSV na pasta indicada.

    Args:
        df_teams: DataFrame consolidado dos times (todas ligas)
        season_id: ID da temporada (ex.: 25673)
        nome_liga: Nome da liga (ex.: "Spain La Liga 2")
        pasta_base: Pasta base de saída (ex.: "season_2025/ligas")
        ano_label: Rótulo do ano (ex.: "2025-2026")

    Returns:
        Caminho do arquivo CSV gerado.
    """
    if 'season_id' not in df_teams.columns:
        raise ValueError("Coluna 'season_id' não encontrada no DataFrame")

    df_out = df_teams[df_teams['season_id'] == season_id].copy()

    # Garante diretório de saída
    os.makedirs(pasta_base, exist_ok=True)

    # Adiciona coluna 'league' com o nome da liga e remove identificadores
    df_out['league'] = nome_liga
    drop_cols = ['team_id', 'season_id']
    df_out = df_out.drop(columns=[c for c in drop_cols if c in df_out.columns])

    slug = _slugify(nome_liga)
    filename = f"{ano_label}_{slug}_teams.csv"
    out_path = os.path.join(pasta_base, filename)

    # Salva
    df_out.to_csv(out_path, index=False, encoding='utf-8')

    return out_path


def export_all_leagues(
    df_teams: pd.DataFrame,
    ligas: List[tuple],
    pasta: str,
) -> List[str]:
    """
    Exporta um CSV por item de `ligas`.

    ligas: lista de tuplas (season_id, nome_liga, ano_label)
    pasta: pasta base (ex.: "season_2025/ligas")

    Returns:
        Lista de caminhos gerados.
    """
    paths: List[str] = []
    os.makedirs(pasta, exist_ok=True)
    for season_id, nome, ano in ligas:
        p = clear_data_teams(df_teams, int(season_id), str(nome), pasta, str(ano))
        paths.append(p)
    return paths
