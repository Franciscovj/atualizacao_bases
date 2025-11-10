import requests
import json
import os
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

API_TOKEN = '8NLOW1LwYgAdVAY4TF5zOOpMEib0tuRJ4uiB4J9j07LpUaqSOVHMwpKokW77'
BASE_URL = "https://api.sportmonks.com/v3/core/types"

def get_all_core_types():
    all_data = []
    url = BASE_URL
    params = {"api_token": API_TOKEN}
    page = 1

    while url:
        print(f"🔄 Buscando página {page}...")
        # Verifica se token está na URL
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        if "api_token" not in query_params:
            # Se não tiver token na URL, passa nos params
            response = requests.get(url, params=params)
        else:
            # Se já tem token na URL, não passa params para evitar duplicação
            response = requests.get(url)

        if response.status_code != 200:
            print(f"❌ Erro na requisição: {response.status_code} - {response.text}")
            break

        data = response.json()
        all_data.extend(data.get("data", []))

        pagination = data.get("pagination", {})
        if pagination.get("has_more"):
            url = pagination.get("next_page")
            page += 1
        else:
            url = None

    os.makedirs("json", exist_ok=True)
    file_path = "json/core_types_all_pages.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump({"data": all_data}, f, ensure_ascii=False, indent=4)

    print(f"\n✅ Dados completos salvos em '{file_path}'")
    print(f"📊 Total de tipos: {len(all_data)}")

if __name__ == "__main__":
    get_all_core_types()
