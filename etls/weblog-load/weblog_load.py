import argparse
import logging
import os

from pyspark.sql.functions import udf
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StructType,
    StructField,
    StringType,
    TimestampType,
)
from utils import (
    write_postgres,
    spark_session,
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", "-p", dest="path", help="The path to the weblog data")
    return parser.parse_args()


def parse_log(log: str):
    from apachelogs import LogParser, COMBINED
    from user_agents import parse
    from ipaddress import ip_address

    parser = LogParser(COMBINED)
    entry = parser.parse(log)
    ipnumber = int(ip_address(entry.remote_host))
    user_agent = parse(entry.headers_in["User-Agent"])

    path = entry.request_line.split(" ")[1]

    ua_str = "MOBILE" if user_agent.is_mobile else "PC"
    if user_agent.device.brand:
        ua_str += f" {user_agent.device.brand}"
    if user_agent.device.model:
        ua_str += f" {user_agent.device.model}"

    return (
        # Keep the original just for comparison
        log,
        entry.remote_host,
        ipnumber,
        path,
        entry.remote_user,
        entry.directives["%t"],
        ua_str,
    )


def main():
    args = parse_args()

    if not args.path or not os.path.exists(args.path):
        raise Exception(f"Error, invalid path {args.path}")

    spark = spark_session("weblog-load")
    schema = StructType(
        [
            StructField("log", StringType(), False),
            StructField("host", StringType(), False),
            StructField("ipnumber", LongType(), False),
            StructField("path", StringType(), False),
            StructField("user", StringType(), True),
            StructField("date_time", TimestampType(), False),
            StructField("device_id", StringType(), False),
        ]
    )

    parse_log_udf = udf(parse_log, schema)
    df = (
        spark.read.text(args.path)
        .select(parse_log_udf("value").alias("parsed"))
        .select("parsed.*")
    )

    write_postgres(df, "weblogs")

    logging.warning("Finished writing to DB!")


main()
