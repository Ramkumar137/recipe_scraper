import boto3
import os

DYNAMO_TABLE = os.getenv("DYNAMO_TABLE_NAME")

dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.getenv("AWS_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

table = dynamodb.Table(DYNAMO_TABLE)


def save_to_dynamodb(recipe_id: str, recipe_json: dict):
    table.put_item(
        Item={
            "recipe_id": recipe_id,
            "data": recipe_json
        }
    )
