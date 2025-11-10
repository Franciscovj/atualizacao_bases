"""
Script principal para processar estatísticas de temporada dos times.

Este script lê `output/teams_to_enrich.json` e para cada (team_id, season_id)
busca estatísticas da temporada na API SportMonks e salva em
`output/teams_season_stats.json`.

Uso:
    python teams_enrich.py

Fluxo de execução:
    1. Carrega times do arquivo JSON
    2. Remove duplicados por (team_id, season_id)
    3. Verifica cache para evitar chamadas repetidas
    4. Faz requisições à API SportMonks
    5. Processa e salva resultados
    6. Atualiza cache
"""

import time
from config import API_TOKEN
from modules.file_handler import (
    load_teams_to_enrich,
    save_team_statistics,
    ensure_output_directory
)
from modules.cache_manager import (
    load_cache,
    save_cache,
    get_from_cache,
    add_to_cache
)
from modules.api_client import (
    fetch_team_season_statistics
)
from modules.data_processor import (
    flatten_statistics,
    load_core_types_mapping,
    deduplicate_teams,
    find_type_ids_by_patterns,
    load_all_statistic_type_ids
)


# ============================================================================
# CONFIGURAÇÕES
# ============================================================================

# Arquivos de entrada/saída
INPUT_TEAMS_FILE = 'output/teams_to_enrich_2025.json'
OUTPUT_TEAM_STATS_FILE = 'output/teams_season_stats.json'

# Arquivos de cache
CACHE_FILE = 'output/cache_team_season.json'

# Parâmetros da API
# Para maximizar o que a API retorna, usaremos filtros com TODOS os tipos de estatística
# (isso depende da cobertura por temporada/competição)
REQUEST_FULL_DETAILS = False  # usa filtros por tipo; tente trazer o máximo possível
TEAM_SEASON_TYPES = []  # Se vazio, será preenchido automaticamente
TIMEOUT_S = 8  # Timeout em segundos
MAX_PAGES_FALLBACK = 2  # Máximo de páginas no fallback
BACKOFF_S = 0.6  # Delay entre requisições (rate limiting)

# Seleção automática de tipos por padrão (útil para tentar trazer splits "home/away/half")
AUTO_SELECT_TYPES_BY_PATTERNS = True
TYPE_PATTERNS = [
    'home', 'away',
    'first-half', 'second-half', '1st', '2nd', 'half',
    'ht', 'ft'
]

# Carregar todos os tipos de estatística do catálogo para forçar o máximo retorno
USE_ALL_STAT_TYPES = True

# Ignora o cache e força reprocessamento (útil após mudar filtros/tipos)
FORCE_REFRESH = True


# ============================================================================
# FUNÇÕES PRINCIPAIS
# ============================================================================


def enrich_teams_details():
    """Função descontinuada. Este script não suporta mais modo 'enrich'."""
    raise NotImplementedError(
        "Modo 'enrich' removido. Use apenas: python teams_enrich.py"
    )


def process_team_season_statistics():
    """
    Processa estatísticas de temporada para cada time.
    
    Fluxo:
        1. Carrega times de teams_to_enrich.json
        2. Remove duplicados por (team_id, season_id)
        3. Carrega mapeamento de tipos de estatísticas
        4. Para cada time+temporada:
           - Verifica cache
           - Se não estiver em cache, busca via API
           - Achata estatísticas em formato legível
        5. Salva resultado em teams_season_stats.json
        6. Atualiza cache
    
    Endpoint usado:
        GET /v3/football/statistics/seasons/teams/{team_id}/{season_id}
    
    Dados obtidos:
        - Gols marcados/sofridos
        - Vitórias/empates/derrotas
        - Cartões amarelos/vermelhos
        - Finalizações, posse de bola, etc
    """
    print("\n" + "="*60)
    print("📊 PROCESSAMENTO DE ESTATÍSTICAS DE TEMPORADA")
    print("="*60)
    
    # Passo 1: Garante que diretório de saída existe
    ensure_output_directory('output')

    # Passo 2: Carrega times do arquivo
    try:
        teams = load_teams_to_enrich(INPUT_TEAMS_FILE)
    except FileNotFoundError:
        print(f"❌ Arquivo {INPUT_TEAMS_FILE} não encontrado!")
        return

    # Passo 3: Remove duplicados por (team_id, season_id)
    unique_teams = deduplicate_teams(teams)
    print(f"📊 Times únicos (team_id, season_id): {len(unique_teams)}")

    # Passo 4: Carrega mapeamento de tipos de estatísticas
    core_map = load_core_types_mapping()
    
    # Passo 5: Carrega cache existente
    cache = load_cache(CACHE_FILE)
    print(f"💾 Estatísticas já em cache: {len(cache)}")

    results = []
    total = len(unique_teams)
    
    # Construção da lista de tipos a solicitar quando não estamos em full details
    selected_types = TEAM_SEASON_TYPES[:] if TEAM_SEASON_TYPES else []
    if not REQUEST_FULL_DETAILS:
        # 1) Todos os tipos de estatísticas disponíveis no catálogo
        if USE_ALL_STAT_TYPES and not selected_types:
            all_types = load_all_statistic_type_ids('json/core_types_all_pages.json')
            selected_types = all_types
            if selected_types:
                print(f"🧩 Tipos carregados do catálogo (statistic): {len(selected_types)} IDs")

        # 2) União com padrões (home/away/metades) para tentar garantir splits
        if AUTO_SELECT_TYPES_BY_PATTERNS:
            auto_types = find_type_ids_by_patterns(
                file_path='json/core_types_all_pages.json',
                patterns=TYPE_PATTERNS,
                model_type='statistic'
            )
            # Evita duplicados
            merged = sorted(set((selected_types or []) + auto_types))
            if merged:
                selected_types = merged
                print(f"🔎 União de tipos com padrões: {len(selected_types)} IDs")
    # Passo 6: Processa cada time
    for i, ((team_id, season_id), info) in enumerate(unique_teams.items(), start=1):
        cache_key = f"{team_id}:{season_id}"
        
        # Verifica cache primeiro
        cached_stats = None if FORCE_REFRESH else get_from_cache(cache, cache_key)
        if not cached_stats:
            # Busca estatísticas da API
            details = fetch_team_season_statistics(
                team_id=team_id,
                season_id=season_id,
                api_token=API_TOKEN,
                request_full_details=REQUEST_FULL_DETAILS,
                team_season_types=selected_types if not REQUEST_FULL_DETAILS else TEAM_SEASON_TYPES,
                timeout=TIMEOUT_S,
                max_pages=MAX_PAGES_FALLBACK
            )
            
            # Achata estatísticas em formato legível
            flat_stats = flatten_statistics(details, core_map)
            
            # Adiciona ao cache
            add_to_cache(cache, cache_key, flat_stats)
            
            # Log de progresso
            team_name = info.get('team_name') or info.get('name') or str(team_id)
            print(f"✅ {i}/{total} - {team_name} ({team_id}) temporada {season_id} - {len(flat_stats)} stats")
        else:
            flat_stats = cached_stats
        
        # Adiciona aos resultados
        results.append({
            'team_id': team_id,
            'season_id': season_id,
            'team_name': info.get('team_name') or info.get('name') or '',
            'stats': flat_stats
        })

    # Passo 7: Salva resultado final
    save_team_statistics(results, OUTPUT_TEAM_STATS_FILE)
    
    # Passo 8: Salva cache
    save_cache(cache, CACHE_FILE)
    
    print(f"\n🎯 Estatísticas de temporada salvas em {OUTPUT_TEAM_STATS_FILE}")


# ============================================================================
# PONTO DE ENTRADA
# ============================================================================

if __name__ == '__main__':
    """
    Ponto de entrada do script (somente estatísticas de temporada).
    Uso: python teams_enrich.py
    """
    process_team_season_statistics()
