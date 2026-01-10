import requests
from urllib.parse import quote
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from app.config import ASTRA_DB_URL, ASTRA_DB_TOKEN

HEADERS = {
    "X-Cassandra-Token": ASTRA_DB_TOKEN,
    "Content-Type": "application/json"
}

PRIMARY_COLLECTION = "recipes_primary"
SECONDARY_COLLECTION = "recipes_secondary"


def _encode_id(raw_id: str) -> str:
    return quote(raw_id, safe="")


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
def _astra_put(url: str, payload: dict):
    r = requests.put(url, headers=HEADERS, json=payload, timeout=15)
    r.raise_for_status()


def save_to_primary_astra(recipe_id: str, recipe_json: dict):
    encoded_id = _encode_id(recipe_id)

    payload = {
        "_id": encoded_id,
        "data": recipe_json,
        "$vectorize": recipe_json.get("title", "")
    }

    url = f"{ASTRA_DB_URL}/collections/{PRIMARY_COLLECTION}/documents/{encoded_id}"
    _astra_put(url, payload)


def save_to_secondary_astra(recipe_id: str, text_for_vector: str):
    encoded_id = _encode_id(recipe_id)

    payload = {
        "_id": encoded_id,
        "ref_id": encoded_id,
        "$vectorize": text_for_vector
    }

    url = f"{ASTRA_DB_URL}/collections/{SECONDARY_COLLECTION}/documents/{encoded_id}"
    _astra_put(url, payload)
