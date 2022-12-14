import argparse
import logging
import os

from datetime import datetime
from pyspark.sql.functions import count
from utils import (
    read_postgres,
    write_postgres,
    write_csv,
    spark_session,
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-devices",
        dest="devices",
        help="How many unique devices to calculate",
        default="5",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = spark_session("popular-devices")
    num_devices = args.devices

    weblogs = read_postgres(spark, "weblogs")

    """
    SELECT device_id, COUNT(*) as entries
    FROM weblogs
    GROUP BY device_id
    LIMIT {num_devices}
    """
    popular_devices = (
        weblogs.groupBy("device_id")
        .agg(count("*").alias("entries"))
        .orderBy("device_id")
        .limit(int(num_devices))
    )

    now = datetime.now().strftime("%Y-%m-%d")
    write_postgres(popular_devices, "popular_devices")
    write_csv(popular_devices, f"/reports/popular_devices/date={now}")

    logging.warning("Finished writing to DB!")


main()
