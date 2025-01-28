import json
import os

import requests


def save_json(data: dict, filename: str) -> None:
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


def get_page(url: str, offset: int = 0, retries: int = 3) -> dict:
    try:
        response = requests.get(f'{url}offset={offset}')
        response.raise_for_status()
        return response.json()
    except Exception as e:
        if retries > 0:
            return get_page(url, offset, retries - 1)
        raise e


def process_page(url: str, offset: int, base_folder: str) -> None:
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)
    filename = os.path.join(base_folder, f'{offset}.json')
    if os.path.exists(filename):
        print(f'Page - {offset} - {filename} - Already exists')
        return
    try:
        data = get_page(url, offset)
        save_json(data, filename)
        print(f'Page - {offset} - {filename} - Success')
    except Exception as e:
        print(f'Page - {offset} - {filename} - Error: {e}')


def get_cpf_data(url: str, codigo: str, token: str, retries: int = 3) -> dict:
    try:
        response = requests.get(url, params={"codigo": codigo}, headers={"chave-api-dados": token})
        response.raise_for_status()
        return response.json()
    except Exception as e:
        if retries > 0:
            return get_cpf_data(url, codigo, token, retries - 1)
        raise e


def process_cpf(url: str, codigo: str, token: str, base_folder: str) -> None:
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)
    filename = os.path.join(base_folder, f'{codigo}.json')
    if os.path.exists(filename):
        print(f'CPF - {codigo} - {filename} - Already exists')
        return
    try:
        data = get_cpf_data(url, codigo, token)
        save_json(data, filename)
        print(f'CPF - {codigo} - {filename} - Success')
    except Exception as e:
        print(f'CPF - {codigo} - {filename} - Error: {e}')


def validate_cnjp_file(file_path: str) -> bool:
    if not os.path.exists(file_path):
        return False
    with open(file_path, 'r') as file:
        try:
            data = json.load(file)
        except Exception as e:
            return False
        if os.path.getsize(file_path) == 0:
            return False
        elif json.dumps(data) in ["{}", "[]"]:
            return False
        elif data.get("error", None) is not None:
            return False
        return True


def get_cnpj_data(url: str, cnpj: str, retries: int = 3) -> dict:
    try:
        response = requests.get(f"{url}/{cnpj}.json")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        if retries > 0:
            return get_cnpj_data(url, cnpj, retries - 1)
        raise e


def process_cnpj(url: str, cnpj: str, base_folder: str) -> None:
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)
    filename = os.path.join(base_folder, f'{cnpj}.json')
    if validate_cnjp_file(filename):
        print(f'CNPJ - {cnpj} - {filename} - Already exists')
        return
    try:
        data = get_cnpj_data(url, cnpj)
        save_json(data, filename)
        print(f'CNPJ - {cnpj} - {filename} - Success')
    except Exception as e:
        print(f'CNPJ - {cnpj} - {filename} - Error: {e}')
