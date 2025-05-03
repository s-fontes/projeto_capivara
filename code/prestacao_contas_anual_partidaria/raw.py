import os
import re
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile

import sys
sys.path.append(os.path.abspath(__file__ + "/../../.."))
from common import get_path

BASE_URL = f"https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas_anual_partidaria/prestacao_contas_anual_partidaria_{{year}}.zip"

TARGET_FOLDER = get_path("/data/prestacao_contas_anual_partidaria/raw/")
if not os.path.isdir(TARGET_FOLDER):
    os.makedirs(TARGET_FOLDER)


def process_file(url: str, folder: str) -> None:

    try:
        response = urlopen(url)
        zip_file = ZipFile(BytesIO(response.read()))
        zip_file.extractall(folder)
        print(f"Downloaded and extracted {url} to {folder}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")


def clean_file(file_path: str) -> None:

    patterns = [
        #Internal double quotes → single quote
        (r'(?<!^)(?<![\n;])"(?![\n;$])', "'"),
        #Spaces before and after double quotes → remove
        (r'(?<=\")\s+(?=\w)|(?<=\w)\s+(?=\")', ''),
        #Multiple spaces → single space
        (r'[^\S\n]+', ' '),
        #Decimals like "123,45" → "123.45"
        (r'(\")(\d+),(\d+)(\")', r'\1\2.\3\4'),
        #Decimals like ",12" → "0.12"
        (r'(\"),(\d+)(\")', r'\g<1>0.\2\3')
    ]

    with open(file_path, "r", encoding="latin1") as file:
        content = file.read()

    for pattern, replacement in patterns:
        content = re.sub(pattern, replacement, content)

    with open(file_path, "w", encoding="utf-8") as file:
        file.write(content)

    print(f"Cleaned {file_path} successfully")


def main():
    years = list(range(2017, 2025))
    urls_folders = [
        (BASE_URL.format(year=year), os.path.join(TARGET_FOLDER, str(year)))
        for year in years
    ]
    with ThreadPoolExecutor() as executor:
        executor.map(lambda x: process_file(*x), urls_folders)
    print("All files downloaded and extracted successfully")

    csv_files = [
        os.path.join(root, file) for root, _, files in os.walk(TARGET_FOLDER)
        for file in files
        if file.endswith(".csv")
    ]

    with Pool() as executor:
        executor.map(clean_file, csv_files)
    print("All files cleaned successfully")



if __name__ == "__main__":
    print("Debug pourpose only")
    main()
