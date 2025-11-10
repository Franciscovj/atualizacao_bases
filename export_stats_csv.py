"""
Gera um CSV a partir de output/teams_season_stats.json

Colunas: team_id, season_id, team_name, e todas as chaves de stats como colunas subsequentes.

Uso:
    python export_stats_csv.py
Saída:
    output/teams_season_stats.csv
"""
import json
import csv
import os
from typing import List, Dict, Any

INPUT_JSON = os.path.join('output', 'teams_season_stats.json')
OUTPUT_CSV = os.path.join('output', 'teams_season_stats_2025.csv')


def load_json(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def collect_columns(rows: List[Dict[str, Any]]) -> List[str]:
    """Coleta todas as chaves possíveis do campo 'stats' e retorna colunas ordenadas."""
    stat_keys = set()
    for row in rows:
        stats = row.get('stats') or {}
        if isinstance(stats, dict):
            stat_keys.update(stats.keys())
    # Ordena as chaves para ter colunas estáveis
    ordered = sorted(stat_keys)
    return ordered


def write_csv(rows: List[Dict[str, Any]], stat_columns: List[str], out_path: str) -> None:
    base_cols = ['team_id', 'season_id', 'team_name']
    header = base_cols + stat_columns

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in rows:
            stats = row.get('stats') or {}
            out_row = {
                'team_id': row.get('team_id'),
                'season_id': row.get('season_id'),
                'team_name': row.get('team_name'),
            }
            # Preenche colunas de stats; valores ausentes ficam vazios
            for k in stat_columns:
                v = stats.get(k)
                out_row[k] = v
            writer.writerow(out_row)


def main():
    print(f"📥 Lendo {INPUT_JSON}")
    rows = load_json(INPUT_JSON)
    print(f"🔎 Registros: {len(rows)}")

    print("🧩 Coletando colunas de stats...")
    stat_columns = collect_columns(rows)
    print(f"🧱 Total de colunas de stats: {len(stat_columns)}")

    print(f"💾 Gravando CSV em {OUTPUT_CSV}")
    write_csv(rows, stat_columns, OUTPUT_CSV)
    print("✅ Concluído!")


if __name__ == '__main__':
    main()
