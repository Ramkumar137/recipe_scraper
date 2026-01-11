import requests
from app.config import (
    ASTRA_PRIMARY_DB_URL,
    ASTRA_PRIMARY_DB_TOKEN,
    ASTRA_SECONDARY_DB_URL,
    ASTRA_SECONDARY_DB_TOKEN,
)

PRIMARY_HEADERS = {
    "X-Cassandra-Token": ASTRA_PRIMARY_DB_TOKEN,
    "Content-Type": "application/json",
}

SECONDARY_HEADERS = {
    "X-Cassandra-Token": ASTRA_SECONDARY_DB_TOKEN,
    "Content-Type": "application/json",
}

PRIMARY_COLLECTION = "recipes_primary"
SECONDARY_COLLECTION = "recipes_secondary"


def save_to_primary_astra(recipe_id: int, recipe_json: dict, vector_text: str):
    url = f"{ASTRA_PRIMARY_DB_URL}/collections/{PRIMARY_COLLECTION}/documents/{recipe_id}"

    _delete_if_exists(url, PRIMARY_HEADERS)

    payload = {
        "_id": recipe_id,
        "recipe": recipe_json,
        "$vectorize": vector_text,
    }

    r = requests.put(url, headers=PRIMARY_HEADERS, json=payload, timeout=15)
    r.raise_for_status()



def save_to_secondary_astra(recipe_id: int, vector_text: str):
    url = f"{ASTRA_SECONDARY_DB_URL}/collections/{SECONDARY_COLLECTION}/documents/{recipe_id}"

    _delete_if_exists(url, SECONDARY_HEADERS)

    payload = {
        "_id": recipe_id,
        "ref_id": recipe_id,
        "$vectorize": vector_text,
    }

    r = requests.put(url, headers=SECONDARY_HEADERS, json=payload, timeout=15)
    r.raise_for_status()



def _delete_if_exists(url: str, headers: dict):
    r = requests.delete(url, headers=headers, timeout=10)
    # 404 is OK (means not existing)
    if r.status_code not in (200, 204, 404):
        r.raise_for_status()
