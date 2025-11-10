import json
import os
from typing import Dict, Tuple

# Paths
FIXTURES_FILE = 'output/fixtures_expandidos.json'
TEAM_STATS_FILE = 'output/teams_season_stats.json'
OUTPUT_FILE = 'output/fixtures_expandidos_enriched.json'


def load_json(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def coerce_season_id(val):
    if isinstance(val, int):
        return val
    if isinstance(val, str) and val.isdigit():
        return int(val)
    try:
        return int(val)
    except Exception:
        return None


def build_team_lookup(team_stats: list) -> Dict[Tuple[int, int], dict]:
    idx = {}
    for item in team_stats:
        team_id = item.get('team_id')
        season_id = item.get('season_id')
        stats = item.get('stats') or {}
        if team_id is None or season_id is None:
            continue
        idx[(team_id, season_id)] = stats
    return idx


def merge_fixtures_with_team_stats(fixtures: list, team_idx: Dict[Tuple[int, int], dict]) -> list:
    merged = []
    inject_count = 0
    for fx in fixtures:
        season_id = coerce_season_id(fx.get('season_id'))
        home_id = fx.get('home_team_id')
        away_id = fx.get('away_team_id')

        if season_id and home_id:
            h_stats = team_idx.get((home_id, season_id))
            if isinstance(h_stats, dict):
                for k, v in h_stats.items():
                    fx[f'home_season_{k}'] = v
                inject_count += 1
        if season_id and away_id:
            a_stats = team_idx.get((away_id, season_id))
            if isinstance(a_stats, dict):
                for k, v in a_stats.items():
                    fx[f'away_season_{k}'] = v
                inject_count += 1
        merged.append(fx)
    return merged, inject_count


def main():
    os.makedirs('output', exist_ok=True)
    if not os.path.exists(FIXTURES_FILE):
        print(f"❌ {FIXTURES_FILE} não encontrado. Rode hist_games.py antes para gerar as partidas.")
        return
    if not os.path.exists(TEAM_STATS_FILE):
        print(f"❌ {TEAM_STATS_FILE} não encontrado. Rode teams_enrich.py antes para gerar estatísticas de temporada.")
        return

    fixtures = load_json(FIXTURES_FILE)
    team_stats = load_json(TEAM_STATS_FILE)

    team_idx = build_team_lookup(team_stats)
    merged, count = merge_fixtures_with_team_stats(fixtures, team_idx)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print(f"✅ Merge concluído: {count} injeções de season stats em fixtures.")
    print(f"📄 Salvo em {OUTPUT_FILE}")


if __name__ == '__main__':
    main()
