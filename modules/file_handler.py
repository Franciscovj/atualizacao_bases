"""
Gerenciamento de arquivos de entrada e saída.

Este módulo contém funções para:
- Ler arquivos JSON de entrada
- Salvar resultados em JSON
- Criar diretórios necessários
"""

import json
import os
from typing import List, Dict, Any


def load_teams_to_enrich(file_path: str) -> List[Dict[str, Any]]:
    """
    Carrega lista de times a serem enriquecidos.
    
    Args:
        file_path: Caminho do arquivo JSON com os times
        
    Returns:
        Lista de dicionários com team_id, season_id, team_name
        
    Raises:
        FileNotFoundError: Se o arquivo não existir
        
    Exemplo:
        >>> teams = load_teams_to_enrich('output/teams_to_enrich.json')
        >>> print(teams[0])
        {'team_id': 3169, 'season_id': 25673, 'team_name': 'Albacete'}
        
    Formato esperado do arquivo:
        [
            {
                "team_id": 3169,
                "season_id": 25673,
                "team_name": "Albacete",
                "location": "away"
            }
        ]
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            teams = json.load(f)
        
        print(f"✅ Carregados {len(teams)} times de {file_path}")
        return teams
        
    except json.JSONDecodeError as e:
        print(f"❌ Erro ao decodificar JSON de {file_path}: {e}")
        raise
    except Exception as e:
        print(f"❌ Erro ao ler arquivo {file_path}: {e}")
        raise


def save_team_statistics(
    results: List[Dict[str, Any]],
    output_file: str
) -> None:
    """
    Salva estatísticas dos times em arquivo JSON.
    
    Args:
        results: Lista de dicionários com estatísticas dos times
        output_file: Caminho do arquivo de saída
        
    Exemplo:
        >>> results = [
        ...     {
        ...         "team_id": 3169,
        ...         "season_id": 25673,
        ...         "team_name": "Albacete",
        ...         "stats": {"goals-scored": 15}
        ...     }
        ... ]
        >>> save_team_statistics(results, 'output/teams_season_stats.json')
        
    Formato do arquivo gerado:
        [
            {
                "team_id": 3169,
                "season_id": 25673,
                "team_name": "Albacete",
                "stats": {
                    "goals-scored": 15,
                    "goals-conceded": 10,
                    "wins": 8
                }
            }
        ]
    """
    try:
        # Garante que o diretório existe
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Salvos {len(results)} times em {output_file}")
        
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo {output_file}: {e}")
        raise


def save_enriched_teams(
    teams: List[Dict[str, Any]],
    output_file: str
) -> None:
    """
    Salva times enriquecidos com informações completas.
    
    Args:
        teams: Lista de times com dados completos
        output_file: Caminho do arquivo de saída
        
    Exemplo:
        >>> teams = [
        ...     {
        ...         "team_id": 3169,
        ...         "name": "Albacete",
        ...         "country": {"id": 32, "name": "Spain"},
        ...         "venue": {"name": "Estadio Carlos Belmonte"}
        ...     }
        ... ]
        >>> save_enriched_teams(teams, 'output/teams_enriched.json')
    """
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(teams, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Salvos {len(teams)} times enriquecidos em {output_file}")
        
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo {output_file}: {e}")
        raise


def ensure_output_directory(directory: str = 'output') -> None:
    """
    Garante que o diretório de saída existe.
    
    Args:
        directory: Caminho do diretório a criar
        
    Exemplo:
        >>> ensure_output_directory('output')
    """
    os.makedirs(directory, exist_ok=True)
