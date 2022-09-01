import psycopg2
import logging

from os import getenv


logging.basicConfig(format="%(asctime)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S")
log = logging.getLogger(__name__)

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
    path = f"/b2b/ddl/{model}.sql"
    if not path:
        raise Exception(f"Unable to find {path}")

    with open(path) as f:
        create_ddl = f.read()

    #log.warning(f"Running create query:\n{create_ddl}")
    cursor.execute(create_ddl)


def load_table(cursor, model: str):
    path = f"/b2b/data/{model}.csv"
    with open(path) as f:
        next(f)
        cursor.copy_from(f, model, sep=',')

def sample_table(cursor, model: str):
    sample_query = f"SELECT * FROM \"{model}\" LIMIT 2;"

    #log.warning(f"Running sample query:\n{sample_query}")
    cursor.execute(sample_query)

    return [l for l in cursor.fetchall()]

def seed():
    MODELS = {
        "company",
        "customer",
        "product",
        "order",
        "inventory",
        "sales",
    }

    try:
        conn = setup_connection()
        cursor = conn.cursor()

        for model in MODELS:
            create_table(cursor, model)

            load_table(cursor, model)

            sample = sample_table(cursor, model)

            log.warning(f"Successfully created {model}. Sampled data:\n{sample}")

        conn.commit()

    finally:
        conn.close()
        log.warning("Done")

if __name__ == "__main__":
    seed()
