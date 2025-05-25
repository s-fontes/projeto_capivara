import os
import json
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count, Pool
from logging import getLogger
from typing import Any


logger = getLogger()


CONTRATOS_URL = "https://pncp.gov.br/api/consulta/v1/contratos"
DATA_FOLDER = "./data/contratos_pncp"
os.makedirs(DATA_FOLDER, exist_ok=True)


class MaxRetriesExceeded(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class DateInterval:
    def __init__(self, start_date: datetime, end_date: datetime):
        self._start_date = start_date
        self._end_date = end_date

    @property
    def start_date(self):
        return self._start_date.strftime("%Y%m%d")

    @property
    def end_date(self):
        return self._end_date.strftime("%Y%m%d")

    def __str__(self):
        return f"(start_date: {self.start_date}, end_date: {self.end_date})"

    def __repr__(self):
        return f"DateInterval(start_date={self.start_date}, end_date={self.end_date})"


class Page:
    def __init__(self, date_interval: DateInterval, page_number: str):
        self.date_interval = date_interval
        self.page_number = page_number

    @property
    def file_path(self):
        return os.path.join(
            DATA_FOLDER,
            f"{self.date_interval.start_date}_{self.date_interval.end_date}_{self.page_number}.json"
        )

    @property
    def params(self):
        return {
            "dataInicial": self.date_interval.start_date,
            "dataFinal": self.date_interval.end_date,
            "pagina": self.page_number
        }

    def __str__(self):
        return f"(page: {self.page_number}, date_interval: {self.date_interval})"


def save_page(data: dict, filename: str) -> None:
    logger.info(f"Saving file: {filename}")
    with open(filename, "w") as file:
        json.dump(data.get("data", []), file, indent=4, ensure_ascii=False)
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


def process_date_interval(date_interval: DateInterval) -> None:
    logger.info(f"Processing date range: {date_interval}")
    pages = get_page(
        Page(
            date_interval=date_interval,
            page_number="1"
        )
    ).get("totalPaginas")
    if pages is None:
        logger.warning(
            f"No pages found for date range: {date_interval}"
        )
        return
    offset_list = list(range(1, pages + 1))
    pages = [Page(date_interval, str(i)) for i in offset_list]
    logger.info(f"Number of pages: {len(pages)}")
    with ThreadPoolExecutor(max_workers=min(cpu_count(), 8) * 3) as pool:
        pool.map(process_page, pages)
    logger.info(f"Date range processed: {date_interval}")


def get_date_intervals(initial_date: datetime, final_date: datetime, date_window: timedelta) -> list[DateInterval]:
    intersection_span = 24
    intervals = []
    while initial_date < final_date:
        end_date = initial_date + date_window
        intervals.append(
            DateInterval(
                initial_date,
                end_date
            )
        )
        initial_date = end_date - timedelta(days=intersection_span)
    return intervals


def main():
    try:
        logger.info("Starting processing")
        initial_date = datetime(2021, 1, 1)
        final_date = datetime.now()
        date_window = timedelta(days=120)
        date_intervals = get_date_intervals(
            initial_date, final_date, date_window
        )
        logger.info(f"Number of date intervals: {len(date_intervals)}")
        logger.info(f"Date intervals: {date_intervals}")
        with Pool(processes=min(cpu_count(), 8)) as pool:
            pool.map(process_date_interval, date_intervals)
        logger.info("Processing completed")
    except Exception:
        logger.exception("Error in main processing")
        raise


if __name__ == "__main__":
    main()
