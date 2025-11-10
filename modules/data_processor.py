"""
Processamento e transformação de dados da API SportMonks.

Este módulo contém funções para:
- Extrair e estruturar dados dos times
- Achatar estatísticas em formato legível
- Mapear tipos de estatísticas para nomes amigáveis
"""

import json
import os
from typing import Dict, List, Any, Optional


def flatten_statistics(
    details: List[Dict[str, Any]],
    type_id_to_name: Dict[str, str]
) -> Dict[str, float]:
    """
    Converte lista de estatísticas da API em dicionário plano EXPANDIDO.

    A API retorna estatísticas em formato complexo:
    [
        {"type_id": 42, "value": {"total": 15, "home": 8, "away": 7}},
        {"type_id": 45, "value": {"average": 56}},
        {"type_id": 52, "value": 10}
    ]

    Saída expandida (máximo de informações numéricas):
    {
        "shots-total": 15,                 # se existir 'total'
        "shots-total.total": 15,
        "shots-total.home": 8,
        "shots-total.away": 7,
        "ball-possession.average": 56,
        "goals": 10
    }

    Regras:
    - Se "value" for dict: exporta TODAS as chaves numéricas como
      "{stat_name}.{subkey}" (ex.: shots-total.home)
    - Se houver "total", mantém também a chave agregada "{stat_name}" = total
    - Se "value" for número simples, usa "{stat_name}" diretamente
    - Ignora valores não numéricos

    Args:
        details: Lista de estatísticas da API (campo 'details')
        type_id_to_name: Mapeamento de type_id para nome legível

    Returns:
        Dicionário com estatísticas achatadas e expandidas

    Exemplo:
        >>> details = [{"type_id": 42, "value": {"total": 15, "home": 8}}]
        >>> mapping = {"42": "shots-total"}
        >>> flatten_statistics(details, mapping)
        {'shots-total': 15, 'shots-total.total': 15, 'shots-total.home': 8}
    """
    flattened: Dict[str, float] = {}

    for detail in details:
        type_id = detail.get('type_id')

        # Nome legível do tipo (ex: "shots-total")
        stat_name = (
            type_id_to_name.get(str(type_id))
            or type_id_to_name.get(int(type_id))
            or str(type_id)
        )

        value = detail.get('value')

        # Caso 1: dicionário com múltiplos subvalores
        if isinstance(value, dict):
            # Exporta todas as chaves numéricas
            for subkey, subval in value.items():
                if isinstance(subval, (int, float)):
                    flattened[f"{stat_name}.{subkey}"] = subval

            # Mantém agregada "{stat_name}" como 'total' se existir
            if 'total' in value and isinstance(value['total'], (int, float)):
                flattened[stat_name] = value['total']
            else:
                # Caso não exista 'total', tenta uma prioridade de agregação
                for k in ("average", "goals", "in", "out", "highest", "lowest"):
                    if isinstance(value.get(k), (int, float)):
                        flattened[stat_name] = value[k]
                        break

        # Caso 2: número simples
        elif isinstance(value, (int, float)):
            flattened[stat_name] = value

        # Outros tipos são ignorados

    return flattened


def extract_team_details(api_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extrai e estrutura informações completas de um time.
    
    Converte a resposta bruta da API em formato estruturado e limpo.
    
    Args:
        api_data: Dados brutos retornados pela API (campo 'data')
        
    Returns:
        Dicionário estruturado com informações do time
        
    Exemplo:
        >>> api_data = {"id": 3169, "name": "Albacete", "country": {...}}
        >>> team = extract_team_details(api_data)
        >>> print(team['name'])  # "Albacete"
        
    Estrutura retornada:
        {
            "team_id": int,
            "name": str,
            "short_code": str,
            "image_path": str (URL do logo),
            "founded": int (ano),
            "type": str ("domestic" ou "national"),
            "placeholder": bool,
            "country": {
                "id": int,
                "name": str,
                "image_path": str
            },
            "venue": {
                "id": int,
                "name": str,
                "city": str,
                "capacity": int,
                "image_path": str,
                "address": str
            },
            "coach": {
                "id": int,
                "name": str,
                "full_name": str,
                "nationality": str,
                "image_path": str
            }
        }
    """
    enriched = {
        'team_id': api_data.get('id'),
        'name': api_data.get('name'),
        'short_code': api_data.get('short_code'),
        'image_path': api_data.get('image_path'),
        'founded': api_data.get('founded'),
        'type': api_data.get('type'),
        'placeholder': api_data.get('placeholder', False),
        'country': None,
        'venue': None,
        'coach': None
    }
    
    # Extrai informações do país
    if 'country' in api_data and api_data['country']:
        country = api_data['country']
        enriched['country'] = {
            'id': country.get('id'),
            'name': country.get('name'),
            'image_path': country.get('image_path')
        }
    
    # Extrai informações do estádio
    if 'venue' in api_data and api_data['venue']:
        venue = api_data['venue']
        enriched['venue'] = {
            'id': venue.get('id'),
            'name': venue.get('name'),
            'city': venue.get('city_name'),
            'capacity': venue.get('capacity'),
            'image_path': venue.get('image_path'),
            'address': venue.get('address')
        }
    
    # Extrai informações do técnico
    if 'coach' in api_data and api_data['coach']:
        coach = api_data['coach']
        enriched['coach'] = {
            'id': coach.get('id'),
            'name': coach.get('common_name') or coach.get('display_name'),
            'full_name': coach.get('display_name'),
            'nationality': coach.get('nationality'),
            'image_path': coach.get('image_path')
        }
    
    return enriched


def load_core_types_mapping(file_path: str = 'json/core_types_all_pages.json') -> Dict[str, str]:
    """
    Carrega mapeamento de IDs de tipos de estatísticas para nomes legíveis.
    
    A API SportMonks usa IDs numéricos para tipos de estatísticas:
    - 42: Goals scored
    - 52: Goals conceded
    - 86: Wins
    
    Este arquivo mapeia esses IDs para nomes amigáveis.
    
    Args:
        file_path: Caminho do arquivo JSON com os tipos
        
    Returns:
        Dicionário mapeando type_id (str) para nome (str)
        
    Exemplo:
        >>> mapping = load_core_types_mapping()
        >>> print(mapping['42'])  # "goals-scored"
        
    Formato do arquivo esperado:
        {
            "data": [
                {
                    "id": 42,
                    "code": "GOALS_SCORED",
                    "name": "Goals Scored"
                }
            ]
        }
    """
    if not os.path.exists(file_path):
        print(f"⚠️  Arquivo de tipos não encontrado: {file_path}")
        return {}
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        
        mapping = {}
        for item in payload.get('data', []):
            type_id = item.get('id')
            
            # Tenta obter nome do código, developer_name ou name
            code = (item.get('code') or 
                   item.get('developer_name') or 
                   item.get('name') or '').strip()
            
            if not code:
                continue
            
            # Normaliza o nome: "GOALS_SCORED" -> "goals-scored"
            normalized_name = code.replace(' ', '-').replace('_', '-').lower()
            mapping[str(type_id)] = normalized_name
        
        print(f"✅ Carregados {len(mapping)} tipos de estatísticas")
        return mapping
        
    except Exception as e:
        print(f"❌ Erro ao carregar tipos de {file_path}: {e}")
        return {}


def deduplicate_teams(teams: List[Dict[str, Any]]) -> Dict[tuple, Dict[str, Any]]:
    """
    Remove times duplicados baseado em (team_id, season_id).
    
    Args:
        teams: Lista de times com team_id e season_id
        
    Returns:
        Dicionário com chave (team_id, season_id) e valor sendo os dados do time
        
    Exemplo:
        >>> teams = [
        ...     {"team_id": 3169, "season_id": 25673, "name": "Albacete"},
        ...     {"team_id": 3169, "season_id": 25673, "name": "Albacete"}  # duplicado
        ... ]
        >>> unique = deduplicate_teams(teams)
        >>> print(len(unique))  # 1
    """
    unique = {}
    for team in teams:
        key = (team.get('team_id'), team.get('season_id'))
        if key not in unique:
            unique[key] = team
    
    return unique


def find_type_ids_by_patterns(
    file_path: str = 'json/core_types_all_pages.json',
    patterns: List[str] = None,
    model_type: str = 'statistic'
) -> List[int]:
    """
    Busca IDs de tipos de estatísticas cujo nome/código contenha quaisquer padrões.

    Útil para selecionar tipos específicos, como splits "home"/"away".

    Args:
        file_path: Caminho para o JSON de tipos
        patterns: Lista de padrões (minúsculos) a procurar em code/name/developer_name
        model_type: Filtra pelo campo "model_type" (ex.: 'statistic')

    Returns:
        Lista de IDs (int) dos tipos que casam os padrões e model_type

    Exemplo:
        >>> find_type_ids_by_patterns(patterns=['home','away'])
        [101, 102, 203, ...]
    """
    if not patterns:
        return []
    if not os.path.exists(file_path):
        print(f"⚠️  Arquivo de tipos não encontrado: {file_path}")
        return []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
    except Exception as e:
        print(f"❌ Erro ao ler {file_path}: {e}")
        return []

    pats = [p.lower() for p in patterns]
    out: List[int] = []

    for item in payload.get('data', []):
        if model_type and item.get('model_type') != model_type:
            continue
        text = ' '.join([
            str(item.get('code') or ''),
            str(item.get('name') or ''),
            str(item.get('developer_name') or ''),
        ]).lower()
        if any(p in text for p in pats):
            tid = item.get('id')
            if isinstance(tid, int):
                out.append(tid)

    return out


def load_all_statistic_type_ids(file_path: str = 'json/core_types_all_pages.json') -> List[int]:
    """
    Retorna todos os IDs de tipos cujo `model_type` == 'statistic'.

    Args:
        file_path: Caminho do JSON de tipos

    Returns:
        Lista de IDs (int)

    Exemplo:
        >>> ids = load_all_statistic_type_ids()
        >>> len(ids)
        120  # por exemplo
    """
    if not os.path.exists(file_path):
        print(f"⚠️  Arquivo de tipos não encontrado: {file_path}")
        return []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
    except Exception as e:
        print(f"❌ Erro ao ler {file_path}: {e}")
        return []

    out: List[int] = []
    for item in payload.get('data', []):
        if item.get('model_type') == 'statistic' and isinstance(item.get('id'), int):
            out.append(item['id'])
    return out
