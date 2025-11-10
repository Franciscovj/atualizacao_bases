from typing import Dict, Any, Tuple
from .utils import to_float


def odds_1x2_result(odds: list, bookmaker_id: int) -> Tuple[Any, Any, Any]:
    home_odd = draw_odd = away_odd = None
    for odd in odds:
        if odd.get('bookmaker_id') == bookmaker_id and odd.get('market_id') == 1:
            label = str(odd.get('label', '')).lower()
            value = to_float(odd.get('value'))
            if label == 'home':
                home_odd = value
            elif label == 'draw':
                draw_odd = value
            elif label == 'away':
                away_odd = value
    return home_odd, draw_odd, away_odd


def odds_btts_result(odds: list, bookmaker_id: int, bookmaker: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for odd in odds:
        if odd.get('bookmaker_id') == bookmaker_id and odd.get('market_id') == 14:
            label = str(odd.get('label', '')).lower()
            if label in ['yes', 'no']:
                result[f"odds_btts_{bookmaker}_{label}"] = to_float(odd.get('value'))
    return result


def odds_over_under_result(odds: list, bookmaker_id: int, bookmaker: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for odd in odds:
        if odd.get('bookmaker_id') == bookmaker_id and odd.get('market_id') == 7:
            label = str(odd.get('label', '')).lower()
            line = odd.get('total') or odd.get('line')
            # FILTRO ESPECÍFICO PARA market_id 7: permitir apenas totais 2.0, 2.25 e 2.5
            if str(line) not in ["2.0", "2.25", "2.5","2","2.0, 2.5"]:
                continue  # Ignorar odds fora do intervalo permitido
            if line is None:
                continue
            line_clean = str(line).replace('.', '_')
            if label in ['over', 'under']:
                result[f"odds_{label}_{bookmaker}_{line_clean}"] = to_float(odd.get('value'))
    return result
