import argparse
import logging
import os

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", dest="path", help="The path to the data")
    parser.add_argument(
        "--header", dest="header", help="Whether or not the CSV path contains a header"
    )
    parser.add_argument("--model", dest="model", help="The name of the model to load")
    return parser.parse_args()


def spark_session():
    return SparkSession.builder.master("local[*]").appName("csv-load").getOrCreate()


def main():
    args = parse_args()

    if not args.path or not os.path.exists(args.path) or not args.model:
        raise Exception(f"Error, invalid args {args.path}, {args.model}")

    spark = spark_session()

    reader = spark.read

    if args.header and args.header == "true":
        reader = reader.option("header", "true")

    for model in args.model.split(","):
        logging.warning(f"Sinking model {model}")

        reader.csv(f"{args.path}/{model}.csv").write.option(
            "driver", "org.postgresql.Driver"
        ).jdbc(
            url="jdbc:postgresql://app-db:5432/challenge",
            table=f'"{model}"',
            mode="overwrite",
            properties={
                "user": "postgres",
                "password": "postgres",
            },
        )

    logging.warning("Finished writing to DB!")


main()
