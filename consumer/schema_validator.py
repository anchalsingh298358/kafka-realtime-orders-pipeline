import json
import os


SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "schemas",
    "order_schema.json"
)


def load_schema():

    with open(SCHEMA_PATH, "r") as f:
        return json.load(f)


schema = load_schema()


def validate_order(order):

    required_fields = [field["name"] for field in schema["fields"]]

    for field in required_fields:
        if field not in order:
            raise ValueError(f"Missing field: {field}")

    for field in schema["fields"]:

        name = field["name"]
        expected_type = field["type"]

        if expected_type == "int" and not isinstance(order[name], int):
            raise ValueError(f"{name} must be integer")

        if expected_type == "double" and not isinstance(order[name], (int, float)):
            raise ValueError(f"{name} must be numeric")

        if expected_type == "string" and not isinstance(order[name], str):
            raise ValueError(f"{name} must be string")

    return True