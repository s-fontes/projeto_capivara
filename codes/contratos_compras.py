import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from logging import getLogger
from typing import Any


logger = getLogger()


CONTRATOS_URL = "https://compras.dados.gov.br/comprasContratos/v1/contratos.json"
DATA_FOLDER = "./data/contratos_compras"
BASE_FOLDER = os.path.join(DATA_FOLDER, "raw")
os.makedirs(BASE_FOLDER, exist_ok=True)


class MaxRetriesExceeded(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class Page:
    def __init__(self, offset: int):
        self.offset = offset

    @property
    def file_path(self):
        return os.path.join(
            BASE_FOLDER,
            f"{self.offset}.json"
        )

    @property
    def params(self):
        return {
            "offset": self.offset,
            "order": "asc"
        }

    def __str__(self):
        return f"(offset: {self.offset})"


def save_page(data: dict, filename: str) -> None:
    logger.info(f"Saving file: {filename}")
    with open(filename, "w") as file:
        json.dump(
            data.get("_embedded", {}).get("contratos", []),
            file,
            indent=4,
            ensure_ascii=False
        )
    logger.info(f"File saved: {filename}")


def get_page(page: Page, retries: int = 5) -> Any:
    logger.info(f"Getting page: {page} - Retries left: {retries}")

    response = requests.get(CONTRATOS_URL, params=page.params)
    logger.info(f"Request URL: {response.url}")

    if response.status_code == 200:
        logger.info(f"Page: {page} - Fetched successfully")
        return response.json()

    elif response.status_code == 204:
        logger.info(f"Page: {page} - No content")
        return {}

    else:
        logger.error(
            f"Error fetching page: {page} - Status code: {response.status_code}"
        )
        if retries > 0:
            return get_page(page, retries - 1)
        else:
            logger.error(f"Retries exhausted for page: {page}")
            raise MaxRetriesExceeded(
                f"url: {response.url} - status_code: {response.status_code} - message: {response.text}"
            )


def process_page(page: Page) -> None:
    logger.info(f"Processing page: {page}")
    if os.path.exists(page.file_path):
        logger.info(f"File already exists: {page.file_path}")
        return
    try:
        data = get_page(page)
        save_page(data, page.file_path)
        logger.info(f"Page processed: {page}")
    except MaxRetriesExceeded:
        logger.exception(f"Error fetching page: {page}")
    except Exception:
        logger.exception(f"Error processing page: {page}")
        raise


def contratos_compras():
    try:
        logger.info("Starting processing")
        max_count = get_page(Page(0)).get("count")
        if max_count is None:
            raise ValueError("Could not get the number of contracts")
        logger.info(f"Max count: {max_count}")

        offset_list = [Page(offset) for offset in range(0, max_count, 500)]
        logger.info(f"Offset list len: {len(offset_list)}")
        with ThreadPoolExecutor(max_workers=min(cpu_count() * 4, 32)) as executor:
            executor.map(
                process_page,
                offset_list
            )

        logger.info("Processing completed")
    except Exception:
        logger.exception("Error in main processing")
        raise


if __name__ == "__main__":
    contratos_compras()
