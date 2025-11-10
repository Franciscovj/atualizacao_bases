import requests
import json
from urllib.parse import urlparse, parse_qs
import os
from modules.stats_games import extract_value_from_statistics
from modules.odds import odds_1x2_result, odds_btts_result, odds_over_under_result
from modules.db import save_fixtures_to_mongo, MongoSink
from modules.mapping import get_type_name
from modules.api_iterate import iterate_fixtures_pages
from config import API_TOKEN, START_DATE, END_DATE, INCLUDE, FILTERS, LOCALE, ORDER, PER_PAGE, MONGO_COLLECTION

BASE_URL = f"https://api.sportmonks.com/v3/football/fixtures/between/{START_DATE}/{END_DATE}"

# Fast mode flags
# Set to True if you need referee and events (slower). False keeps it fast.
USE_EVENTS_AND_REFEREE = False

PARAMS = {
    "api_token": API_TOKEN,
    "include": INCLUDE,
    "filters": FILTERS,
    "locale": LOCALE,
    "order": ORDER,
    "per_page": PER_PAGE
}

os.makedirs('output', exist_ok=True)
OUTPUT_FILE = 'output/fixtures_expandidos.json'

# helpers moved to modules.utils

# Phase 1 only: fast match scraping (no season enrichment in this script)
SEASON_ENRICH = False

# Mapping moved to modules.mapping
page_number = 1
all_fixtures = []
mongo_sink = None

print(f"📥 Buscando partidas entre {START_DATE} e {END_DATE} e salvando em JSON...")

try:
    # Collect unique teams seen to enrich later in a separate step
    teams_seen = {}
    # Open Mongo sink early to persist incrementally
    try:
        # Usar a coleção configurada em config.py
        mongo_sink = MongoSink(collection_name=MONGO_COLLECTION)
        mongo_ok = True
    except Exception as me:
        print(f"⚠️ MongoDB indisponível para persistência incremental: {me}")
        mongo_ok = False

    for fixtures_page, pagination in iterate_fixtures_pages(BASE_URL, PARAMS, timeout=8):

        page_docs = []
        for fixture in fixtures_page:
            fixture_id = fixture.get("id")

            league = fixture.get('league', {})
            league_name = league.get('name', '')
            league_country_id = league.get('country_id', '')
            season = fixture.get('season_id', {})

            home_team = away_team = home_position = away_position = ''
            home_team_id = None
            away_team_id = None
            for p in fixture.get('participants', []):
                meta = p.get('meta', {})
                if meta.get('location') == 'home':
                    home_team = p.get('name', '')
                    home_position = meta.get('position', '')
                    try:
                        home_team_id = p.get('id')
                    except Exception:
                        home_team_id = None
                elif meta.get('location') == 'away':
                    away_team = p.get('name', '')
                    away_position = meta.get('position', '')
                    try:
                        away_team_id = p.get('id')
                    except Exception:
                        away_team_id = None

            odds = fixture.get('odds', [])

            # odds helpers moved to modules.odds

            stats = fixture.get("statistics", [])
            goals = extract_value_from_statistics(stats, type_id=52)

            # Iterate only over type_ids that actually appear in this fixture
            present_type_ids = {s.get('type_id') for s in stats if isinstance(s, dict) and s.get('type_id') is not None}
            dynamic_stats = {}
            for tid in present_type_ids:
                name = get_type_name(tid)
                home_val = extract_value_from_statistics(stats, tid, 'home')
                away_val = extract_value_from_statistics(stats, tid, 'away')

                # Add side-specific values when present
                if home_val is not None:
                    dynamic_stats[f'home_{name}'] = home_val
                if away_val is not None:
                    dynamic_stats[f'away_{name}'] = away_val

                # If both sides are missing, try a global (no location) value and emit a single key
                if home_val is None and away_val is None:
                    for s in stats:
                        if s.get('type_id') == tid and s.get('location') not in ('home', 'away'):
                            gval = (s.get('data') or {}).get('value')
                            if gval is not None:
                                dynamic_stats[name] = gval
                            break

            home_odd, draw_odd, away_odd = odds_1x2_result(odds, bookmaker_id=20)
            odds_btts = odds_btts_result(odds, bookmaker_id=20, bookmaker='pinnacle')
            odds_over_under = odds_over_under_result(odds, bookmaker_id=20, bookmaker='pinnacle')
            home_odd_365, draw_odd_365, away_odd_365 = odds_1x2_result(odds, bookmaker_id=2)
            odds_btts_365 = odds_btts_result(odds, bookmaker_id=2, bookmaker='bet365')
            odds_over_under_365 = odds_over_under_result(odds, bookmaker_id=2, bookmaker='bet365')            


            doc = {
                'id': fixture_id,
                'name': fixture.get('name', ''),
                'starting_at': fixture.get('starting_at', ''),
                'result_info': fixture.get('result_info', ''),
                'league_name': league_name,
                'league_country_id': league_country_id,
                'season_id': season,
                'home_team': home_team,
                'home_team_id': home_team_id,
                'away_team': away_team,
                'away_team_id': away_team_id,
                'home_goals': goals['home'],
                'away_goals': goals['away'],
                'away_position': away_position,
                'home_position': home_position,
                'odds_pinnacle_home': home_odd,
                'odds_pinnacle_draw': draw_odd,
                'odds_pinnacle_away': away_odd,
                'odds_bet365_home': home_odd_365,
                'odds_bet365_draw': draw_odd_365,
                'odds_bet365_away': away_odd_365
            }

            doc.update(dynamic_stats)
            doc.update(odds_btts)
            doc.update(odds_over_under)
            doc.update(odds_btts_365)
            doc.update(odds_over_under_365)

            # Record teams to enrich later
            try:
                season_id_val = season if isinstance(season, int) else fixture.get('season_id')
                if home_team_id and season_id_val:
                    teams_seen[(home_team_id, season_id_val)] = {
                        'team_id': home_team_id,
                        'season_id': season_id_val,
                        'team_name': home_team,
                        'location': 'home'
                    }
                if away_team_id and season_id_val:
                    teams_seen[(away_team_id, season_id_val)] = {
                        'team_id': away_team_id,
                        'season_id': season_id_val,
                        'team_name': away_team,
                        'location': 'away'
                    }
            except Exception:
                pass

            all_fixtures.append(doc)
            page_docs.append(doc)

        # Persist this page immediately to Mongo
        if mongo_ok and page_docs:
            try:
                res = mongo_sink.upsert_many(page_docs)
                if res:
                    print(f"🗄️ Mongo (página {page_number}): upserted={res['upserted']}, matched={res['matched']}, modified={res['modified']}")
            except Exception as pe:
                print(f"⚠️ Falha ao persistir página no Mongo: {pe}")

        print(f"✅ Página {page_number} com {len(fixtures_page)} partidas processadas.")
        has_more = pagination.get("has_more", False)
        if has_more:
            print("🔁 Indo para próxima página...")
        else:
            print("🏁 Última página atingida.")
        page_number += 1

    # Salva tudo em JSON
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_fixtures, f, ensure_ascii=False, indent=4)

    # Salva IDs de times/temporadas para enriquecimento posterior
    teams_list = list(teams_seen.values())
    with open('output/teams_to_enrich.json', 'w', encoding='utf-8') as tf:
        json.dump(teams_list, tf, ensure_ascii=False, indent=4)

    # Persist into MongoDB (final summary) - optional, since we persisted per page
    try:
        # Usar a coleção configurada em config.py
        msg = save_fixtures_to_mongo(all_fixtures, collection_name=MONGO_COLLECTION)
        print(f"🗄️ {msg}")
    except Exception as e:
        print(f"⚠️ Falha ao salvar no MongoDB (final): {e}")

    print(f"\n🎉 Finalizado! Total de partidas salvas: {len(all_fixtures)} em {OUTPUT_FILE}")
    print(f"📝 Times para enriquecer salvos em output/teams_to_enrich.json (total: {len(teams_list)})")

except requests.exceptions.RequestException as err:
    print(f"❌ Erro: {err}")
finally:
    if mongo_sink:
        try:
            mongo_sink.close()
        except Exception:
            pass
