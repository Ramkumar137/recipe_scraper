import requests
import uuid
from app.config import ASTRA_DB_URL, ASTRA_DB_TOKEN

HEADERS = {
    "Authorization": f"Bearer {ASTRA_DB_TOKEN}",
    "Content-Type": "application/json"
}

PRIMARY_COLLECTION = "recipes_primary"
SECONDARY_COLLECTION = "recipes_secondary"


def save_to_primary_astra(recipe_json: dict) -> str:
    """
    Stores full recipe JSON + vector
    Returns recipe_id
    """
    recipe_id = str(uuid.uuid4())

    payload = {
        "_id": recipe_id,
        "data": recipe_json,
        "$vectorize": recipe_json.get("title", "")
    }

    url = f"{ASTRA_DB_URL}/collections/{PRIMARY_COLLECTION}/documents"
    r = requests.post(url, headers=HEADERS, json=payload, timeout=10)
    r.raise_for_status()

    return recipe_id


def save_to_secondary_astra(recipe_id: str, text_for_vector: str):
    """
    Stores only recipe_id + vector
    """
    payload = {
        "_id": recipe_id,
        "ref_id": recipe_id,
        "$vectorize": text_for_vector
    }

    url = f"{ASTRA_DB_URL}/collections/{SECONDARY_COLLECTION}/documents"
    r = requests.post(url, headers=HEADERS, json=payload, timeout=10)
    r.raise_for_status()
