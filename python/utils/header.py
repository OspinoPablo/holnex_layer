import json

def params_parser(value):
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value