import json
import os
from typing import Dict

# Base minimal mapping
_BASE_MAP: Dict[int, str] = {
    41: "shots-off-target", 42: "shots-total", 86: "shots-on-target",
    49: "shots-insidebox", 50: "shots-outsidebox", 58: "shots-blocked",
    53: "goals-kicks", 54: "goal-attempts", 64: "hit-woodwork",
    580: "big-chances-created", 581: "big-chances-missed",
    43: "attacks", 44: "dangerous-attacks",
    45: "ball-possession", 80: "passes", 81: "successful-passes",
    82: "successful-passes-percentage", 116: "accurate-passes", 117: "key-passes",
    62: "long-passes", 63: "short-passes",
    122: "long-balls", 123: "long-balls-won", 124: "through-balls", 125: "through-balls-won",
    98: "total-crosses", 99: "accurate-crosses",
    78: "tackles", 106: "duels-won", 107: "aeriels-won",
    100: "interceptions", 101: "clearances", 102: "clearances-won", 65: "successful-headers",
    105: "total-duels",
    84: "yellowcards", 83: "redcards", 85: "yellowred-cards",
    56: "fouls", 51: "offsides", 60: "throwins", 34: "corners", 55: "free-kicks", 59: "substitutions",
    57: "saves", 104: "saves-insidebox", 103: "punches",
    79: "assists", 87: "injuries", 47: "penalties", 46: "ball-safe", 70: "headers",
    118: "rating", 120: "touches",
    32: "probability", 33: "valuebet",
    5304: "expected-goals", 5305: "expected-goals-on-target",
    7939: "expected-points", 7943: "expected-non-penalty-goals",
    7945: "expected-goals-open-play", 7944: "expected-goals-set-play",
    7942: "expected-goals-corners", 7941: "expected-goals-free-kicks", 7940: "expected-goals-penalties",
    1584: "accurate-passes-percentage", 1605: "successful-dribbles-percentage",
}


def load_core_types_mapping(path: str = 'json/core_types_all_pages.json') -> Dict[int, str]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        ext: Dict[int, str] = {}
        for item in payload.get('data', []):
            try:
                tid = int(item.get('id'))
            except Exception:
                continue
            code = (item.get('code') or item.get('developer_name') or item.get('name') or '').strip()
            if not code:
                continue
            cname = code.replace(' ', '-').replace('_', '-').lower()
            ext[tid] = cname
        return ext
    except Exception:
        return {}


# Build exported mapping once
TYPE_ID_TO_NAME: Dict[int, str] = {**_BASE_MAP, **load_core_types_mapping()}


def get_type_name(tid: int) -> str:
    return TYPE_ID_TO_NAME.get(tid, str(tid))
