from typing import Dict, Generator, List, Tuple, Optional
from urllib.parse import urlparse, parse_qs
import requests


def iterate_fixtures_pages(base_url: str, params: Dict, timeout: int = 8) -> Generator[Tuple[List[dict], Dict], None, None]:
    """
    Iterate paginated fixture pages. Yields (fixtures_page, pagination_dict)
    base_url can already contain query params including api_token or not.
    If api_token is already inside the URL, we do not append params again.
    """
    next_url = base_url
    while next_url:
        parsed = urlparse(next_url)
        query_params = parse_qs(parsed.query)
        if "api_token" in query_params:
            response = requests.get(next_url, timeout=timeout)
        else:
            response = requests.get(next_url, params=params, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        fixtures_page = data.get("data", [])
        pagination = data.get("pagination", {})
        yield fixtures_page, pagination
        next_url = pagination.get("next_page") if pagination.get("has_more") else None
