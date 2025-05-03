import os
import json
import polars as pl


def save_json(data: dict, filename: str) -> None:
    """
    Save a dictionary to a JSON file.
    Args:
        data (dict): The dictionary to save.
        filename (str): The name of the file to save the dictionary to.
    """

    # Ensure the directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # Save the dictionary to a JSON file
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


def normalize_column(column: str) -> pl.Expr:
    """
    Normalize a column by stripping whitespace, converting to uppercase,
    replacing special characters with their ASCII equivalents.
    Args:
        column (str): The name of the column to normalize.
    Returns:
        pl.Expr: A Polars expression that normalizes the column.
    """

    return (
        pl.col(column)
        .str.strip_chars()
        .str.to_uppercase()
        .str.replace_all(r"Á|À|Ã|Â", "A")
        .str.replace_all(r"É|È|Ẽ|Ê", "E")
        .str.replace_all(r"Í|Ì|Ĩ|Î", "I")
        .str.replace_all(r"Ó|Ò|Õ|Ô", "O")
        .str.replace_all(r"Ú|Ù|Ũ|Û|Ü", "U")
        .str.replace_all(r"Ç", "C")
    )


def get_path(path: str) -> str:
    """
    Get the absolute path of a file or directory relative to the project root.
    Args:
        path (str): The relative path to the file or directory.
    Returns:
        str: The absolute path to the file or directory.
    """
    return os.path.abspath(os.path.abspath(__file__ + "/../..") + path)
