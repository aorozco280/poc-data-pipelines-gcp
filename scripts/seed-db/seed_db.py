import argparse
import csv
import logging
import psycopg2

from os import getenv

log = logging.getLogger(__name__)

DDL_PATH = getenv("DDL_PATH") or "/ddl"
DATA_PATH = getenv("DATA_PATH") or "/data"

def setup_connection():
    user = getenv("POSTGRES_USER") or "postgres"
    psw = getenv("POSTGRES_PASSWORD") or "postgres"
    db = getenv("POSTGRES_DB") or "challenge"

    conn = psycopg2.connect(
        database=db,
        user=user,
        password=psw,
        host="database",
        port="5432"
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

def load_table(cursor, model: str):
    path = f"{DATA_PATH}/{model}.csv"
    load_query = f"COPY \"{model}\" FROM '{path}' DELIMITER ',' CSV HEADER;"
    log.warning(f"Executing [{load_query}]")
    cursor.execute(load_query)
    #with open(path, newline='') as f:
    #    reader = csv.reader(f)
    #    for row in reader:
    #        x = 1
    #fmts = ",".join((len(row) * "%s"))
    #values_str = ",".join(cursor.mogrify("(%s,%s,%s)", i).decode('utf-8'))
    #cursor.execute(f"INSERT INTO \"{model}\" VALUES")
    #cursor.copy_from(reader, model, sep=',')

def sample_table(cursor, model: str):
    sample_query = f"SELECT * FROM \"{model}\" LIMIT 2;"

    cursor.execute(sample_query)

    return [l for l in cursor.fetchall()]

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", "-m", dest='model', help="Model to create, either a specific one or a comma-separated list of models")
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
            load_table(cursor, model)
            #sample = sample_table(cursor, model)
            #log.warning(f"Successfully created {model}. Sampled data:\n{sample}")

        conn.commit()

    finally:
        conn.close()
        log.warning("Done")

if __name__ == "__main__":
    args = parse_args()
    seed(args)
