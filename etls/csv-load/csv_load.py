import argparse
import logging
import os

from utils import (
    write_postgres,
    spark_session,
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", dest="path", help="The path to the data")
    parser.add_argument(
        "--header", dest="header", help="Whether or not the CSV path contains a header"
    )
    parser.add_argument("--model", dest="model", help="The name of the model to load")
    return parser.parse_args()


def main():
    args = parse_args()

    if not args.path or not os.path.exists(args.path) or not args.model:
        raise Exception(f"Error, invalid args {args.path}, {args.model}")

    spark = spark_session("csv-load")

    reader = spark.read

    if args.header and args.header == "true":
        reader = reader.option("header", "true")

    for model in args.model.split(","):
        logging.warning(f"Sinking model {model}")

        df = reader.csv(f"{args.path}/{model}.csv")

        write_postgres(df, model)

    logging.warning("Finished writing to DB!")


main()
