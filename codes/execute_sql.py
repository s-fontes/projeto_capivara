import os
import duckdb
from logging import getLogger

logger = getLogger()

DATABASE_PATH = "./database.db"
QUERIES_PATH = "./sql"
DUCKDB_SWAP = "./tmp/duckdb_swap.tmp/"

os.makedirs(os.path.dirname(DUCKDB_SWAP), exist_ok=True)

def delete_swap_directory():
    if os.path.exists(DUCKDB_SWAP):
        try:
            logger.info("Deleting swap directory...")
            for root, dirs, files in os.walk(DUCKDB_SWAP, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(DUCKDB_SWAP)
            logger.info("Swap directory deleted.")
        except OSError:
            logger.exception(f"Error deleting swap directory")
            raise
    else:
        logger.info("Swap directory does not exist, no need to delete.")


def get_connection() -> duckdb.DuckDBPyConnection:
    try:
        logger.info("Connecting to the database...")
        conn = duckdb.connect(database=DATABASE_PATH, read_only=False)
        logger.info("Connected to the database.")
        return conn
    except Exception:
        logger.exception("Error connecting to the database.")
        raise


def get_queries() -> list[str]:
    queries = []
    for root, dirs, files in os.walk(QUERIES_PATH):
        dirs[:] = [d for d in dirs if not d.startswith("#")]
        for file in files:
            if not file.startswith("#") and file.endswith(".sql"):
                queries.append(os.path.join(root, file))
    if not queries:
        logger.warning("No SQL queries found in the specified directory.")
    return queries


def get_max_priority(queries: list[str]) -> int:
    max_priority = 0
    for query in queries:
        try:
            priority = int(os.path.basename(query).split("_")[0])
            if priority >= 0:
                if priority > max_priority:
                    max_priority = priority
            else:
                logger.warning(f"Negative priority in query name: {query}")
        except IndexError:
            logger.warning(f"No priority in query name: {query}")
        except ValueError:
            logger.warning(f"Invalid priority in query name: {query}")
    return max_priority


def get_execution_order() -> list[list[str]]:
    queries = get_queries()
    max_priority = get_max_priority(queries)
    execution_order = [[] for _ in range(max_priority + 1)]
    for query in queries:
        try:
            priority = int(os.path.basename(query).split("_")[0])
            if priority < 0:
                logger.warning(f"Negative priority in query name: {query}")
                continue
            else:
                execution_order[priority].append(query)
        except IndexError:
            logger.warning(f"No priority in query name: {query}")
        except ValueError:
            logger.warning(f"Invalid priority in query name: {query}")
    return execution_order


def execute_queries(conn: duckdb.DuckDBPyConnection):
    execution_order = get_execution_order()
    for queries in execution_order:
        for query in queries:
            try:
                with open(query, "r") as file:
                    sql = file.read()
                logger.info(f"Executing query: {query}")
                resp = conn.execute(sql)
                logger.info(resp.fetchall())
                print(f"Query executed successfully: {query}")
                logger.info(f"Query executed successfully: {query}")
            except Exception:
                logger.exception(f"Error executing query {query}")


def main():
    logger.info("Starting the SQL execution process...")
    conn = get_connection()
    try:
        logger.info("Starting query execution...")
        execute_queries(conn)
        logger.info("All queries executed successfully.")
    finally:
        logger.info("Closing the database connection...")
        conn.close()
        logger.info("Connection closed.")
        delete_swap_directory()
        logger.info("SQL execution process completed.")


if __name__ == "__main__":
    main()
