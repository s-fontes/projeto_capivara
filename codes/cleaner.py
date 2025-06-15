import os
from codes.csv_cleaner import clean_csv
from multiprocessing import Pool, cpu_count

BASE_FOLDER = "/home/fontes/Documentos/repos/projeto_capivara/data/empresas/empresas"

def clean_all_csv_files():
    all_csv_files = []
    for root, _, files in os.walk(BASE_FOLDER):
        for file in files:
            if file.endswith("CSV"):
                all_csv_files.append(os.path.join(root, file))

    with Pool(cpu_count()) as pool:
        pool.map(clean_csv, all_csv_files)