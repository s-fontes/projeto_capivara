import codes.config_log
from logging import getLogger

from codes import (
    bolsa_familia,
    prestacao_contas_anual_partidaria,
    prestacao_de_contas_eleitorais_candidatos
)

logger = getLogger()

def main():

    tasks = [
        bolsa_familia,
        prestacao_contas_anual_partidaria,
        prestacao_de_contas_eleitorais_candidatos
    ]
    for task in tasks:
        try:
            logger.info(f"Starting task: {task.__name__}")
            task()
            logger.info(f"Task completed: {task.__name__}")
        except Exception as e:
            logger.exception(f"Error in task {task.__name__}: {e}")
            raise


    logger.info("All tasks completed successfully.")

if __name__ == "__main__":
    try:
        logger.info("Starting the script.")
        main()
        logger.info("Script finished successfully.")
    except Exception:
        logger.exception("An error occurred during execution.")
        raise

