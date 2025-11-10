"""
Módulo de gerenciamento de cache para dados da API SportMonks.

Este módulo fornece funções para carregar e salvar cache de:
- Estatísticas de temporada dos times
- Detalhes completos dos times

O cache evita chamadas repetidas à API e melhora a performance.
"""

import json
import os
from typing import Dict, Any


def load_cache(cache_file: str) -> Dict[str, Any]:
    """
    Carrega o cache de um arquivo JSON.
    
    Args:
        cache_file: Caminho do arquivo de cache
        
    Returns:
        Dict contendo os dados em cache, ou dict vazio se não existir
        
    Exemplo:
        >>> cache = load_cache('output/cache_team_season.json')
        >>> print(len(cache))  # Número de itens em cache
    """
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️  Erro ao carregar cache de {cache_file}: {e}")
            return {}
    return {}


def save_cache(cache: Dict[str, Any], cache_file: str) -> None:
    """
    Salva o cache em um arquivo JSON.
    
    Args:
        cache: Dicionário com os dados a serem salvos
        cache_file: Caminho do arquivo de cache
        
    Exemplo:
        >>> cache = {'3169:25673': {'goals': 15}}
        >>> save_cache(cache, 'output/cache_team_season.json')
    """
    try:
        # Garante que o diretório existe
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️  Erro ao salvar cache em {cache_file}: {e}")


def get_from_cache(cache: Dict[str, Any], key: str) -> Any:
    """
    Obtém um item do cache pela chave.
    
    Args:
        cache: Dicionário de cache
        key: Chave do item a buscar
        
    Returns:
        Valor do cache ou None se não existir
        
    Exemplo:
        >>> cache = {'3169:25673': {'goals': 15}}
        >>> stats = get_from_cache(cache, '3169:25673')
        >>> print(stats['goals'])  # 15
    """
    return cache.get(key)


def add_to_cache(cache: Dict[str, Any], key: str, value: Any) -> None:
    """
    Adiciona um item ao cache.
    
    Args:
        cache: Dicionário de cache
        key: Chave do item
        value: Valor a ser armazenado
        
    Exemplo:
        >>> cache = {}
        >>> add_to_cache(cache, '3169:25673', {'goals': 15})
    """
    cache[key] = value
