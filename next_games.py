import asyncio
import httpx
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import aiofiles
import csv
import pandas as pd

API_TOKEN = "jf6TyXI1w2ZO2hZQyptvKVtH4jx4Dtu32sN7ZoJbUGF66oKKlddWytXqxKjI"

TARGET_BOOKMAKERS = [20, 2]
TARGET_MARKETS = [1, 7, 14]
TZ = ZoneInfo("America/Sao_Paulo")


def get_date_range():
    agora = datetime.now(TZ)
    start = agora.strftime("%Y-%m-%d")
    end = (agora + timedelta(days=4)).strftime("%Y-%m-%d")
    return start, end, agora


def ensure_token(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    query["api_token"] = [API_TOKEN]
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


async def fetch_all_fixtures(client):
    start, end, agora = get_date_range()

    LEAGUES_URL = f'https://api.sportmonks.com/v3/football/leagues?api_token={API_TOKEN}'
    leagues = {}
    url = LEAGUES_URL

    while url:
        url = ensure_token(url)
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
        for league in data.get('data', []):
            leagues[league['id']] = {
                'name': league.get('name'),
                'short_code': league.get('short_code')
            }
        url = data.get('pagination', {}).get('next_page')

    FIXTURES_URL = f"https://api.sportmonks.com/v3/football/fixtures/between/{start}/{end}?api_token={API_TOKEN}"
    fixtures = []
    url = FIXTURES_URL

    while url:
        url = ensure_token(url)
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
        for f in data.get("data", []):
            fixtures.append({
                "id": f.get("id"),
                "league_id": f.get("league_id"),
                "league_info": f"{leagues.get(f.get('league_id'), {}).get('name', 'Desconhecido')} - {leagues.get(f.get('league_id'), {}).get('short_code', '')}".strip(' - '),
                "name": f.get("name"),
                "starting_at": f.get("starting_at"),
                "has_odds": f.get("has_odds"),
            })
        url = data.get("pagination", {}).get("next_page")

    future_fixtures = [
        f for f in fixtures
        if datetime.fromisoformat(f["starting_at"].replace('Z', '+00:00')).replace(tzinfo=TZ) >= agora
    ]

    print(f"[INFO] {len(future_fixtures)} jogos futuros encontrados (de {len(fixtures)} totais) entre {start} e {end}.")
    return future_fixtures


async def fetch_odds(client, fixture_id):
    ODDS_URL = f"https://api.sportmonks.com/v3/football/odds/pre-match/fixtures/{fixture_id}?api_token={API_TOKEN}"
    try:
        resp = await client.get(ODDS_URL)
        if resp.status_code == 429:
            print(f"[WARN] Rate limit atingido para fixture {fixture_id}. Aguardando {RATE_LIMIT_SLEEP//60} minutos...")
            return None

        resp.raise_for_status()
        data = resp.json().get("data", [])

        bookmaker_names = {20: "Pinnacle", 2: "Bet365"}
        market_names = {1: "Match_Odds", 7: "Lines_Goals", 14: "Btts"}

        filtered = []
        for m in data:
            if m.get("bookmaker_id") not in TARGET_BOOKMAKERS or m.get("market_id") not in TARGET_MARKETS:
                continue

            if m.get("market_id") == 7 and str(m.get("total")) not in ["1.5", "2.5", "3.5"]:
                continue

            filtered.append({
                "id": m.get("id"),
                "fixture_id": m.get("fixture_id"),
                "market_id": m.get("market_id"),
                "bookmaker_id": m.get("bookmaker_id"),
                "book_name": bookmaker_names.get(m.get("bookmaker_id"), "Desconhecido"),
                "market_name": market_names.get(m.get("market_id"), "Desconhecido"),
                "label": m.get("label"),
                "value": m.get("value"),
                "total": m.get("total"),
                "name": m.get("name"),
                "sort_order": m.get("sort_order"),
                "market_description": m.get("market_description"),
                "probability": m.get("probability"),
                "created_at": m.get("created_at"),
                "latest_bookmaker_update": m.get("latest_bookmaker_update"),
            })

        filtered.sort(
            key=lambda x: (
                TARGET_MARKETS.index(x["market_id"]),
                TARGET_BOOKMAKERS.index(x["bookmaker_id"]),
                x["sort_order"]
            )
        )

        unique_books = {o["book_name"] for o in filtered}
        if unique_books:
            line = f"Fixture {fixture_id} -> {', '.join(sorted(unique_books))}\n"
            async with aiofiles.open("bookmakers_with_odds.txt", "a", encoding="utf-8") as f:
                await f.write(line)
            print(f"[INFO] {line.strip()}")

        return filtered

    except Exception as e:
        print(f"[ERRO] Falha ao buscar odds de fixture {fixture_id}: {e}")
        return []


async def track_odds():
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            fixtures = await fetch_all_fixtures(client)
        except Exception as e:
            print(f"[ERRO] Falha ao buscar jogos: {e}")
            return

        games_data = {}

        for fixture in fixtures:
            fixture_id = fixture.get("id")
            odds_data = await fetch_odds(client, fixture_id)

            if odds_data is None:
                await asyncio.sleep(RATE_LIMIT_SLEEP)
                continue

            if fixture_id not in games_data:
                games_data[fixture_id] = {
                    "timestamp": datetime.now(TZ).isoformat(),
                    "fixture_id": fixture_id,
                    "league_info": fixture.get("league_info"),
                    "fixture_name": fixture.get("name"),
                    "starting_at": fixture.get("starting_at"),
                }

            if not odds_data:
                print(f"[INFO] Sem odds para fixture {fixture_id}")
            else:
                for o in odds_data:
                    book_name = o.get("book_name", "")
                    market_name = o.get("market_name", "")
                    label = o.get("label", "")
                    total = o.get("total", "")
                    value = o.get("value", "")

                    if label == "Home":
                        label = "Away"
                    elif label == "Away":
                        label = "Home"

                    if total:
                        col_name = f"{book_name}_{market_name}_{label}_{total}"
                    else:
                        col_name = f"{book_name}_{market_name}_{label}"

                    games_data[fixture_id][col_name] = value

        rows = list(games_data.values())

        all_columns = set()
        for row in rows:
            all_columns.update(row.keys())

        basic_columns = ["league_info", "fixture_name", "starting_at"]
        match_odds_cols = sorted([c for c in all_columns if "Match_Odds" in c])
        btts_cols = sorted([c for c in all_columns if "Btts" in c])
        lines_goals_cols = sorted([c for c in all_columns if "Lines_Goals" in c])
        other_cols = sorted([
            c for c in all_columns
            if c not in basic_columns + match_odds_cols + btts_cols + lines_goals_cols + ["timestamp", "fixture_id"]
        ])

        promo_column = "Para mais mercados visite o site"
        fieldnames = basic_columns + match_odds_cols + btts_cols + lines_goals_cols + other_cols + [promo_column]

        # ✅ Trecho corrigido — agora é código executável
        filtered_rows = []
        for i, row in enumerate(rows):
            filtered_row = {k: v for k, v in row.items() if k in fieldnames}
            if i == 0:
                filtered_row[promo_column] = "https://www.quantumodds.xyz/"
            else:
                filtered_row[promo_column] = ""
            filtered_rows.append(filtered_row)

        try:
            # Criar DataFrame
            df = pd.DataFrame(filtered_rows, columns=fieldnames)
            
            # Salvar como Excel
            with pd.ExcelWriter("season_2025/next_games/next_games.xlsx", engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Odds', index=False)
                
                # Obter a planilha para formatação
                worksheet = writer.sheets['Odds']
                
                # Encontrar a coluna do link promocional
                promo_col_idx = None
                for idx, col in enumerate(fieldnames):
                    if col == promo_column:
                        promo_col_idx = idx + 1  # Excel usa índice 1-based
                        break
                
                # Adicionar hyperlink na primeira linha da coluna promocional
                if promo_col_idx:
                    cell = worksheet.cell(row=2, column=promo_col_idx)  # Row 2 porque row 1 é header
                    cell.hyperlink = "https://www.quantumodds.xyz/"
                    cell.value = "https://www.quantumodds.xyz/"
                    cell.style = "Hyperlink"
            
            print(f"[INFO] Excel salvo com {len(filtered_rows)} linhas para o período selecionado.")
        except Exception as e:
            print(f"[ERRO] Falha ao salvar Excel: {e}")


if __name__ == "__main__":
    asyncio.run(track_odds())
