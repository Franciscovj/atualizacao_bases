"""
Cliente para a API SportMonks.

Este módulo contém todas as funções que fazem requisições HTTP para a API.
Inclui tratamento de erros, retries e rate limiting.
"""

import time
import requests
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse, parse_qs


def fetch_team_season_statistics(
    team_id: int,
    season_id: int,
    api_token: str,
    request_full_details: bool = True,
    team_season_types: List[int] = None,
    timeout: int = 8,
    max_pages: int = 2
) -> List[Dict[str, Any]]:
    """
    Busca estatísticas de um time em uma temporada específica.
    
    Este endpoint retorna dados numéricos como:
    - Gols marcados/sofridos
    - Vitórias/empates/derrotas
    - Cartões amarelos/vermelhos
    - Posse de bola
    - Finalizações
    
    Args:
        team_id: ID do time na API SportMonks
        season_id: ID da temporada
        api_token: Token de autenticação da API
        request_full_details: Se True, busca todos os tipos de estatísticas
        team_season_types: Lista de IDs de tipos específicos (se request_full_details=False)
        timeout: Timeout em segundos para cada requisição
        max_pages: Número máximo de páginas para buscar no fallback
        
    Returns:
        Lista de dicionários com as estatísticas (campo 'details' da API)
        
    Exemplo:
        >>> stats = fetch_team_season_statistics(3169, 25673, "seu_token")
        >>> print(len(stats))  # Número de estatísticas retornadas
        
    Fluxo:
        1. Tenta endpoint direto: /statistics/seasons/teams/{team_id}/{season_id}
        2. Se falhar, usa fallback: /statistics/seasons/teams/{team_id} (paginado)
        3. Filtra pela season_id desejada
    """
    # Tentativa 1: Endpoint direto (mais rápido)
    base_direct = f"https://api.sportmonks.com/v3/football/statistics/seasons/teams/{team_id}/{season_id}"
    params = {"api_token": api_token}
    
    # Adiciona filtros se necessário
    if not request_full_details and team_season_types:
        params["filters"] = "teamstatisticdetailTypes:" + ",".join(str(x) for x in team_season_types)
    
    try:
        response = requests.get(base_direct, params=params, timeout=timeout)
        if response.status_code == 200:
            json_data = response.json()
            data = json_data.get('data') or {}
            
            # Verifica se tem valores válidos
            if data and data.get('has_values'):
                return data.get('details', [])
    except Exception as e:
        print(f"⚠️  Erro no endpoint direto para time {team_id}: {e}")

    # Tentativa 2: Fallback - busca todas as temporadas e filtra
    url = f"https://api.sportmonks.com/v3/football/statistics/seasons/teams/{team_id}"
    params = {
        "api_token": api_token,
        "order": "desc",
        "per_page": 25
    }
    
    if not request_full_details and team_season_types:
        params["filters"] = "teamstatisticdetailTypes:" + ",".join(str(x) for x in team_season_types)

    pages = 0
    details = []
    
    while url and pages < max_pages:
        try:
            # Verifica se a URL já tem api_token
            parsed = urlparse(url)
            query_params = parse_qs(parsed.query)
            
            if 'api_token' in query_params:
                # URL já tem token (próxima página)
                response = requests.get(url, timeout=timeout)
            else:
                # Primeira requisição
                response = requests.get(url, params=params, timeout=timeout)
                
        except Exception as e:
            print(f"⚠️  Erro na página {pages + 1} para time {team_id}: {e}")
            break
        
        if response.status_code != 200:
            print(f"⚠️  Status {response.status_code} na página {pages + 1} para time {team_id}")
            break
        
        data = response.json()
        
        # Processa cada temporada retornada
        for row in data.get('data', []):
            if row.get('season_id') == season_id and row.get('has_values'):
                details.extend(row.get('details', []) or [])
        
        # Verifica se há próxima página
        pagination = data.get('pagination', {})
        url = pagination.get('next_page') if pagination.get('has_more') else None
        pages += 1
    
    return details


def fetch_team_full_details(
    team_id: int,
    api_token: str,
    include: str = "country,venue,coach,sidelined",
    locale: str = "pt",
    timeout: int = 8,
    max_retries: int = 3
) -> Optional[Dict[str, Any]]:
    """
    Busca informações completas de um time.
    
    Este endpoint retorna dados do time como:
    - Nome, código, logo
    - País
    - Estádio (nome, capacidade, cidade)
    - Técnico atual
    - Ano de fundação
    
    Args:
        team_id: ID do time na API SportMonks
        api_token: Token de autenticação da API
        include: Relacionamentos a incluir (separados por vírgula)
        locale: Idioma das respostas (pt, en, es, etc)
        timeout: Timeout em segundos
        max_retries: Número máximo de tentativas em caso de falha
        
    Returns:
        Dicionário com os dados do time, ou None se falhar
        
    Exemplo:
        >>> team = fetch_team_full_details(3169, "seu_token")
        >>> print(team['name'])  # "Albacete"
        >>> print(team['country']['name'])  # "Spain"
        
    Opções de include:
        - country: País do time
        - venue: Estádio
        - coach: Técnico atual
        - sidelined: Jogadores lesionados/suspensos
        - squad: Elenco completo
        - latest: Últimos jogos
        - upcoming: Próximos jogos
        - transfers: Transferências recentes
    """
    url = f"https://api.sportmonks.com/v3/football/teams/{team_id}"
    params = {
        "api_token": api_token,
        "include": include,
        "locale": locale
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            
            if response.status_code == 200:
                data = response.json().get('data', {})
                if data:
                    return data
                    
            elif response.status_code == 404:
                print(f"⚠️  Time {team_id} não encontrado (404)")
                return None
                
            elif response.status_code == 429:
                # Rate limit atingido
                wait_time = 5 * (attempt + 1)
                print(f"⏳ Rate limit atingido, aguardando {wait_time}s...")
                time.sleep(wait_time)
                
            else:
                print(f"⚠️  Erro {response.status_code} ao buscar time {team_id}")
                
        except requests.exceptions.Timeout:
            print(f"⏱️  Timeout ao buscar time {team_id} (tentativa {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Backoff exponencial: 1s, 2s, 4s
                
        except Exception as e:
            print(f"❌ Erro ao buscar time {team_id}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    
    return None
