import requests
from app.config import ASTRA_DB_URL, ASTRA_DB_TOKEN

HEADERS = {
    "Authorization": f"Bearer {ASTRA_DB_TOKEN}",
    "Content-Type": "application/json"
}

PRIMARY_COLLECTION = "recipes_primary"
SECONDARY_COLLECTION = "recipes_secondary"


def save_to_primary_astra(recipe_id: str, recipe_json: dict):
    payload = {
        "_id": recipe_id,
        "data": recipe_json,
        "$vectorize": recipe_json.get("title", "")
    }

    url = f"{ASTRA_DB_URL}/collections/{PRIMARY_COLLECTION}/documents/{recipe_id}"
    r = requests.put(url, headers=HEADERS, json=payload, timeout=10)
    r.raise_for_status()


def save_to_secondary_astra(recipe_id: str, text_for_vector: str):
    payload = {
        "_id": recipe_id,
        "ref_id": recipe_id,
        "$vectorize": text_for_vector
    }

    url = f"{ASTRA_DB_URL}/collections/{SECONDARY_COLLECTION}/documents/{recipe_id}"
    r = requests.put(url, headers=HEADERS, json=payload, timeout=10)
    r.raise_for_status()
