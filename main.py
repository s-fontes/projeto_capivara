import codes.config_log
from logging import getLogger

from codes import (
    # bolsa_familia,
    # prestacao_contas_anual_partidaria,
    # prestacao_de_contas_eleitorais_candidatos,
    # candidatos,
    # contratos_compras,
    # contratos_pncp,
    execute_sql
)

logger = getLogger()
logger.setLevel("INFO")


def main():

    tasks = [
        # bolsa_familia,
        # prestacao_contas_anual_partidaria,
        # prestacao_de_contas_eleitorais_candidatos,
        # candidatos,
        # contratos_compras,
        # contratos_pncp,
        execute_sql
    ]
    for task in tasks:
        try:
            logger.info(f"Starting task: {task.__name__}")
            task()
            logger.info(f"Task completed: {task.__name__}")
        except Exception:
            logger.exception(f"Error in task {task.__name__}")

    logger.info("All tasks completed.")


if __name__ == "__main__":
    try:
        logger.info("Starting the script.")
        main()
        logger.info("Script finished.")
    except Exception:
        logger.exception("An error occurred during execution.")
        raise
