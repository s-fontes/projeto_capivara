import json


def save_json(data: dict, filename: str) -> None:
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
