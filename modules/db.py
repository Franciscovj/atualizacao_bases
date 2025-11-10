from typing import List, Dict, Any
from pymongo import MongoClient, UpdateOne
try:
    from config import MONGO_URI as CFG_URI, MONGO_COLLECTION as CFG_COLL, MONGO_DB_FALLBACK as CFG_DB
except Exception:
    CFG_URI = None
    CFG_COLL = None
    CFG_DB = None

MONGO_URI = CFG_URI or "mongodb://quamtunodds:dropodds@159.198.37.118:27017/sportmonks_data"
#MONGO_URI = CFG_URI or "mongodb://localhost:27017/sportmonks_data"
COLLECTION_NAME = CFG_COLL or "fixtures_2022"
DB_FALLBACK = CFG_DB or "sportmonks_data"


def save_fixtures_to_mongo(documents: List[Dict[str, Any]], collection_name: str = COLLECTION_NAME) -> str:
    if not documents:
        return "MongoDB: lista de partidas vazia, nada salvo."

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Usa explicitamente o banco 'sportmonks_data' em vez de get_default_database()
    db = client[DB_FALLBACK]
    # Cria o banco se não existir (MongoDB cria automaticamente ao inserir)
    db.command('ping')  # Garante que o banco está acessível
    coll = db[collection_name]

    try:
        coll.create_index('id', unique=True)
    except Exception:
        pass

    ops = [
        UpdateOne({'id': doc.get('id')}, {'$set': doc}, upsert=True)
        for doc in documents if doc.get('id') is not None
    ]

    if not ops:
        return "MongoDB: nenhum documento válido para upsert."

    res = coll.bulk_write(ops, ordered=False)
    upserts = getattr(res, 'upserted_count', 0)
    modified = getattr(res, 'modified_count', 0)
    matched = getattr(res, 'matched_count', 0)
    return f"MongoDB gravado: upserts={upserts}, matched={matched}, modified={modified}"


class MongoSink:
    def __init__(self, uri: str = MONGO_URI, collection_name: str = COLLECTION_NAME, db_fallback: str = DB_FALLBACK):
        self._client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        # Ping usando o banco 'sportmonks_data' em vez de 'admin'
        db_for_ping = self._client[db_fallback]
        db_for_ping.command('ping')
        _db = self._client.get_default_database()
        db = _db if _db is not None else self._client[db_fallback]
        self._coll = db[collection_name]
        try:
            self._coll.create_index('id', unique=True)
        except Exception:
            pass

    def upsert_many(self, documents: List[Dict[str, Any]]) -> Dict[str, int] | None:
        if not documents:
            return {"upserted": 0, "matched": 0, "modified": 0}
        ops = [
            UpdateOne({'id': doc.get('id')}, {'$set': doc}, upsert=True)
            for doc in documents if doc.get('id') is not None
        ]
        if not ops:
            return {"upserted": 0, "matched": 0, "modified": 0}
        res = self._coll.bulk_write(ops, ordered=False)
        return {
            "upserted": getattr(res, 'upserted_count', 0),
            "matched": getattr(res, 'matched_count', 0),
            "modified": getattr(res, 'modified_count', 0)
        }

    def close(self):
        try:
            self._client.close()
        except Exception:
            pass
