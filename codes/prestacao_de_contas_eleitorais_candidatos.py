import os
from zipfile import ZipFile
from logging import getLogger
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count, Pool
from functools import partial
from codes.csv_cleaner import clean_csv


logger = getLogger()


DATA_FOLDER = "./data/prestacao_de_contas_eleitorais_candidatos"
ZIP_FOLDER = os.path.join(DATA_FOLDER, "zip")
if not os.path.exists(ZIP_FOLDER):
    raise FileNotFoundError(f"ZIP folder does not exist: {ZIP_FOLDER}")
RAW_FOLDER = os.path.join(DATA_FOLDER, "raw")
os.makedirs(RAW_FOLDER, exist_ok=True)


def extract(file: str, zip_ref: ZipFile) -> None:
    dcc = "despesas_contratadas_candidatos"
    dcc_path = os.path.join(RAW_FOLDER, dcc)
    os.makedirs(dcc_path, exist_ok=True)

    dpc = "despesas_pagas_candidatos"
    dpc_path = os.path.join(RAW_FOLDER, dpc)
    os.makedirs(dpc_path, exist_ok=True)

    rcdo = "receitas_candidatos_doador_originario"
    rcdo_path = os.path.join(RAW_FOLDER, rcdo)
    os.makedirs(rcdo_path, exist_ok=True)

    rc = "receitas_candidatos"
    rc_path = os.path.join(RAW_FOLDER, rc)
    os.makedirs(rc_path, exist_ok=True)

    filename = os.path.basename(file)
    logger.info(f"Extracting {file} from {zip_ref.filename}")
    if filename.startswith(dcc):
        final_path = dcc_path
    elif filename.startswith(dpc):
        final_path = dpc_path
    elif filename.startswith(rcdo):
        final_path = rcdo_path
    elif filename.startswith(rc):
        final_path = rc_path
    else:
        logger.warning(f"Unknown file type: {file}")
        return
    zip_ref.extract(file, final_path)
    logger.info(f"Extracted {file} to {final_path}")
    clean_csv(os.path.join(final_path, file))

def extract_zip(zip_path: str) -> None:
    logger.info(f"Extracting {zip_path} to {RAW_FOLDER}")
    try:
        with ZipFile(zip_path, 'r') as zip_ref:
            logger.info(f"Extracting files from {zip_path}")
            with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
                files = [
                    file for file in zip_ref.namelist() if file.endswith(".csv")
                ]
                extract_zip_ref = partial(
                    extract,
                    zip_ref=zip_ref
                )
                executor.map(extract_zip_ref, files)
        logger.info(f"Extraction completed: {zip_path}")
    except Exception:
        logger.exception(f"Error extracting {zip_path}")


def main():
    logger.info("Starting extraction process")
    actual_files = [
        os.path.join(ZIP_FOLDER, filename)
        for filename in os.listdir(ZIP_FOLDER)
        if filename.endswith(".zip")
    ]
    with Pool(cpu_count()) as executor:
        executor.map(extract_zip, actual_files)
    logger.info("Extraction process completed")


if __name__ == "__main__":
    main()
