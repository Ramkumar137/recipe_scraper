def build_vector_text(recipe_json: dict) -> str:
    """
    Vector text = title + ingredients + time
    """

    parts = []

    title = recipe_json.get("title")
    if isinstance(title, str) and title.strip():
        parts.append(title.strip())

    ingredients = recipe_json.get("ingredients")
    if isinstance(ingredients, list):
        parts.extend(i.strip() for i in ingredients if isinstance(i, str))

    for key in ["cook_time", "prep_time", "total_time"]:
        val = recipe_json.get(key)
        if isinstance(val, str) and val.strip():
            parts.append(val.strip())

    if not parts:
        raise ValueError("No valid text for vectorization")

    return " ".join(parts)[:2000]
