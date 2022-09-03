import argparse
import logging
import os

from pyspark.sql.functions import count
from utils import (
    read_postgres,
    write_postgres,
    spark_session,
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-products",
        dest="products",
        help="How many unique products to calculate",
        default="5",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = spark_session("popular-products")
    num_products = args.products

    tables = [
        "weblogs",
        "ip2geo",
        "sales",
        "customer",
        "order",
        "product",
    ]

    for t in tables:
        read_postgres(spark, t).createOrReplaceTempView(t)

    query = f"""
    SELECT p.id
    FROM sales s
    INNER JOIN `order` o ON s.order_id = o.id
    INNER JOIN customer c ON o.customer_document_number = c.document_number
    INNER JOIN product p ON s.product_id = p.id
    WHERE c.country = (
        SELECT ig.country
        FROM (
            SELECT DISTINCT(ipnumber) as ipnumber
            FROM weblogs
            GROUP BY ipnumber
        ) w
        INNER JOIN ip2geo ig ON w.ipnumber BETWEEN ig.ip_start AND ig.ip_end
        GROUP BY 1
        ORDER BY COUNT(*) DESC
        LIMIT 1
    )
    GROUP BY 1
    ORDER BY COUNT(*) DESC
    LIMIT {num_products}
    """
    df = spark.sql(query)

    write_postgres(df, "popular_products")

    logging.warning("Finished writing to DB!")


main()
