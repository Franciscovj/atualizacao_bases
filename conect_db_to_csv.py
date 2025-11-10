import argparse
import csv
import json
import warnings
from typing import List, Set
from pymongo import MongoClient
from config import MONGO_URI, MONGO_COLLECTION, START_DATE, END_DATE

# Suppress PyMongo no_cursor_timeout session advisory warning in this script
warnings.filterwarnings(
    "ignore",
    message=r".*no_cursor_timeout=True.*",
    category=UserWarning,
    module=r"pymongo(\.synchronous\.collection)?"
)


def is_odds_field(key: str) -> bool:
    k = key.lower()
    return (
        k.startswith('over_')
        or k.startswith('under_')
        or k.startswith('btts_')
        or k in ('home_pinnacle', 'draw_pinnacle', 'away_pinnacle')
    )


def order_fields(all_keys: Set[str], extra_priority: List[str] | None = None) -> List[str]:
    # bring common keys to front if present
    priority = (extra_priority or []) + [
        'id', 'name', 'starting_at', 'result_info',
        'league_name', 'league_country_id', 'season_id',
        'home_team', 'home_team_id', 'away_team', 'away_team_id',
        'home_goals', 'away_goals', 'home_position', 'away_position',
        'home_pinnacle', 'draw_pinnacle', 'away_pinnacle'
    ]
    odds = sorted([k for k in all_keys if is_odds_field(k) and k not in priority])
    non_odds = sorted([k for k in all_keys if (k not in priority and not is_odds_field(k))])

    ordered: List[str] = []
    seen = set()
    for p in priority:
        if p in all_keys and p not in seen:
            ordered.append(p)
            seen.add(p)
    for k in non_odds:
        if k not in seen:
            ordered.append(k)
            seen.add(k)
    for k in odds:
        if k not in seen:
            ordered.append(k)
            seen.add(k)
    return ordered


def main():
    parser = argparse.ArgumentParser(description='Exportar coleção do MongoDB para CSV')
    parser.add_argument('--uri', default=MONGO_URI, help='MongoDB URI (default: config.py)')
    parser.add_argument('--collection', default=MONGO_COLLECTION, help='Nome da coleção (default: config.py)')
    parser.add_argument('--outfile', default=f"output/fixtures_data.csv", help='Arquivo CSV de saída')
    parser.add_argument('--fields', help='Campos separados por vírgula a exportar (opcional)')
    parser.add_argument('--query', help='Filtro em JSON (ex.: {"season_id":23265})')
    parser.add_argument('--batch-size', type=int, default=1000, help='Tamanho do lote do cursor (default: 1000)')
    args = parser.parse_args()

    client = MongoClient(args.uri, serverSelectionTimeoutMS=5000)
    # Usa o banco padrão da URI; evite truth-testing com "or" e compare explicitamente com None
    db_from_uri = client.get_default_database()
    db = db_from_uri if db_from_uri is not None else client['sportmonks_data']
    coll = db[args.collection]

    # Parse query
    mongo_query = {}
    if args.query:
        try:
            mongo_query = json.loads(args.query)
        except Exception as e:
            raise SystemExit(f"Query inválida, não é um JSON: {e}")

    # Determine fields
    field_list: List[str]
    if args.fields:
        field_list = [f.strip() for f in args.fields.split(',') if f.strip()]
        # Start session for the export cursor
        session = client.start_session()
        cursor = coll.find(mongo_query, no_cursor_timeout=True, session=session, batch_size=args.batch_size)
    else:
        # Scan once to collect all keys across documents within a session
        keys: Set[str] = set()
        count_probe = 0
        with client.start_session() as session_probe:
            probe_cursor = coll.find(mongo_query, no_cursor_timeout=True, session=session_probe, batch_size=args.batch_size)
            try:
                for doc in probe_cursor:
                    keys.update(doc.keys())
                    count_probe += 1
                    if count_probe % 5000 == 0:
                        print(f"Descobrindo colunas... analisados {count_probe} documentos")
            finally:
                try:
                    probe_cursor.close()
                except Exception:
                    pass
        if not keys:
            print("Coleção vazia ou sem resultados para o filtro.")
            return
        # Remove _id by default
        keys.discard('_id')
        field_list = order_fields(keys)
        # Start session for the export cursor
        session = client.start_session()
        cursor = coll.find(mongo_query, no_cursor_timeout=True, session=session, batch_size=args.batch_size)

    with open(args.outfile, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=field_list)
        writer.writeheader()
        count = 0
        try:
            for doc in cursor:
                # Remove _id unless explicitly requested
                if '_id' in doc and '_id' not in field_list:
                    del doc['_id']
                writer.writerow({k: doc.get(k) for k in field_list})
                count += 1
                if count % 1000 == 0:
                    print(f"Progresso: {count} linhas...")
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            try:
                session.end_session()
            except Exception:
                pass
    print(f"✅ Exportado {count} documentos para {args.outfile}")


if __name__ == '__main__':
    main()
