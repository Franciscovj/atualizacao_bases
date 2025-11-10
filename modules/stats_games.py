from typing import Dict, List, Optional, Any


def extract_value_from_statistics(statistics: List[dict], type_id: int, location: Optional[str] = None) -> Dict[str, Any] | Any:
    result = {'home': None, 'away': None}
    for stat in statistics:
        if stat.get("type_id") == type_id:
            loc = stat.get("location")
            value = (stat.get("data") or {}).get("value")
            if loc in result:
                result[loc] = value
    if location:
        return result.get(location)
    return result

