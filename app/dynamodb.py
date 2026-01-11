import boto3
import os
from decimal import Decimal

dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.getenv("AWS_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

table = dynamodb.Table(os.getenv("DYNAMO_TABLE_NAME"))


def save_to_dynamodb(recipe_id: int, recipe_json: dict):
    item = {
        "id": str(recipe_id),  # MUST be string
    }

    for key, value in recipe_json.items():
        if isinstance(value, float):
            item[key] = Decimal(str(value))
        else:
            item[key] = value

    table.put_item(Item=item)
