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

    tables = {
        "weblogs",
        "ip2geo",
        "sales",
        "customer",
        "order",
        "product",
    }

    for t in tables:
        read_postgres(spark, t).createOrReplaceTempView(t)

    query = f"""
    WITH unique_logins AS (
        SELECT ipnumber
        FROM weblogs
        WHERE `path` LIKE '%login%'
        GROUP BY 1
    ), most_logged_country AS (
        SELECT ig.country
        FROM unique_logins ul
        INNER JOIN ip2geo ig ON ul.ipnumber BETWEEN CAST(ig.ip_start AS BIGINT) AND CAST(ig.ip_end AS BIGINT)
        GROUP BY 1
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ), agg_purchases AS (
        SELECT c.document_number, c.country, s.product_id, SUM(CAST(quantity AS INT)) as quantity
        FROM sales s
        INNER JOIN `order` o ON s.order_id = o.id
        INNER JOIN customer c ON c.document_number = o.customer_document_number
        GROUP BY 1, 2, 3
    ) SELECT * from most_logged_country;

    SELECT p.name, ap.quantity, ap.country
    FROM agg_purchases ap
    INNER JOIN most_logged_country mlc ON ap.country = mlc.country
    INNER JOIN product p ON ap.product_id = p.id
    ORDER BY 2 DESC
    LIMIT {num_products}
    """
    df = spark.sql(query)

    now = datetime.now().strftime("%Y-%m-%d")
    write_postgres(df, "popular_products")
    write_csv(df, f"/reports/popular_products/date={now}")

    logging.warning("Finished writing to DB!")


main()
