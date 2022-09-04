import argparse
import csv
import logging
import psycopg2

from os import getenv

log = logging.getLogger(__name__)

DDL_PATH = getenv("DDL_PATH") or "/ddl"


def setup_connection():
    user = getenv("POSTGRES_USER") or "postgres"
    psw = getenv("POSTGRES_PASSWORD") or "postgres"
    db = getenv("POSTGRES_DB") or "challenge"

    conn = psycopg2.connect(
        database=db, user=user, password=psw, host="app-db", port="5432"
    )
    conn.autocommit = True

    return conn


def create_table(cursor, model: str):
    path = f"{DDL_PATH}/{model}.sql"
    if not path:
        raise Exception(f"Unable to find {path}")

    with open(path) as f:
        create_ddl = f.read()

    queries = create_ddl.split(";")
    for query in queries:
        if query.strip():
            cursor.execute(f"{query};")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--model",
        "-m",
        dest="model",
        help="Model to create, either a specific one or a comma-separated list of models",
    )
    return parser.parse_args()


def seed(args):
    if not args.model:
        raise Exception("Missing argument --model")

    models = args.model.split(",")
    try:
        conn = setup_connection()
        cursor = conn.cursor()

        for model in models:
            create_table(cursor, model)
            logging.warning("Created table {model}")

        conn.commit()

    finally:
        conn.close()


if __name__ == "__main__":
    args = parse_args()
    seed(args)
