from multiprocessing import Pool

from code.common.utils import process_page, get_page

CONTRATOS_URL = 'https://compras.dados.gov.br/comprasContratos/v1/contratos.json?'
BASE_FOLDER = '../../data/contratos/raw'

if __name__ == '__main__':
    max_count = get_page(CONTRATOS_URL).get('count')
    if max_count is None:
        raise ValueError('Could not get the number of contracts')
    offset_list = list(range(0, max_count, 500))
    with Pool(4) as pool:
        pool.starmap(process_page, [(CONTRATOS_URL, offset, BASE_FOLDER) for offset in offset_list])
