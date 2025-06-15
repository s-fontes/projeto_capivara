import os
import io
import requests
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor
from logging import getLogger
from datetime import datetime
from dateutil.relativedelta import relativedelta

logger = getLogger()

DATA_FOLDER = "./data/empresas"
BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{date}/"
FILES = [
    "Empresas",
    "Estabelecimentos",
    "Socios"
]
INITAL_DATE = datetime.strptime("2023-05", "%Y-%m")
FINAL_DATE = datetime.strptime("2025-05", "%Y-%m")


class Task:
    def __init__(self, url: str, output_folder: str):
        self.url = url
        self.output_folder = output_folder

    def run(self):
        logger.info(f"processing: {self}")
        try:
            response = requests.get(self.url, timeout=60*60, stream=True)
            response.raise_for_status()
        except Exception:
            logger.exception(f"error downloading {self.url}")
            return

        try:
            with ZipFile(io.BytesIO(response.content)) as zip_ref:
                os.makedirs(self.output_folder, exist_ok=True)
                logger.info(f"extracting files from {self.url} to {self.output_folder}")
                zip_ref.extractall(self.output_folder)
                logger.info(f"extraction completed for {self.url}")
        except Exception:
            logger.exception(f"error extracting files from {self.url}")

    def __str__(self):
        return f"Task(url={self.url}, output_folder={self.output_folder})"


def get_files_names(file_name: str, n: int = 9) -> list[str]:
    return [f"{file_name}{i}.zip" for i in range(n + 1)]


def get_dates() -> list[str]:
    data_inicio = INITAL_DATE
    data_fim = FINAL_DATE
    dates = []
    atual = data_inicio
    while atual <= data_fim:
        dates.append(atual.strftime("%Y-%m"))
        atual += relativedelta(months=1)
    return dates


def generate_tasks() -> list[Task]:
    tasks = []
    for date in get_dates():
        for file_name in FILES:
            base_url = BASE_URL.format(date=date)
            for zip_name in get_files_names(file_name):
                url = base_url + zip_name
                output_folder = os.path.join(
                    DATA_FOLDER, file_name.lower(), date)
                tasks.append(Task(url, output_folder))
    return tasks


tasks = generate_tasks()
for task in tasks:
    logger.info(task)


def empresas() -> None:
    logger.info("starting extraction process")
    tasks = generate_tasks()
    logger.info(f"total tasks to run: {len(tasks)}")
    if not tasks:
        logger.warning("no tasks to run")
        return

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(lambda task: task.run(), tasks)

    logger.info("extraction process completed")


if __name__ == "__main__":
    os.makedirs(DATA_FOLDER, exist_ok=True)
    empresas()
