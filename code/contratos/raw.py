import os
from multiprocessing import Pool

import requests

from code.common.utils import save_json

CONTRATOS_URL = 'https://compras.dados.gov.br/comprasContratos/v1/contratos.json?'
BASE_FOLDER = '../../data/contratos/raw'


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


if __name__ == '__main__':
    max_count = get_page(CONTRATOS_URL).get('count')
    if max_count is None:
        raise ValueError('Could not get the number of contracts')
    offset_list = list(range(0, max_count, 500))
    with Pool(4) as pool:
        pool.starmap(process_page, [(CONTRATOS_URL, offset, BASE_FOLDER) for offset in offset_list])
