import argparse
import csv
import numpy
import pendulum
import random

from dataclasses import dataclass, astuple

customer_ids = [1, 2, 3, 5, 8, 9, 10, 11, 12]
product_ids = [n for n in range(1, 14)]
probs = [
    0.21,  # pens
    0.11,  # keyboard
    0.01,  # matress
    0.08,  # guitar
    0.11,  # bottle
    0.03,  # guitar strings
    0.04,  # bass
    0.08,  # lamp
    0.13,  # cocktail kit
    0.01,  # paint
    0.12,  # mouse
    0.05,  # spoon
    0.02,  # wardrobe
]
prices = [
    2.25,
    99.99,
    735.60,
    480.35,
    46.77,
    22.88,
    841.35,
    112.4,
    42.99,
    230.99,
    87.5,
    75.55,
    915.50,
]


@dataclass
class Sale:
    order_id: int
    product_id: int
    quantity: int
    price_per_unit: str
    total: str


@dataclass
class Order:
    id: int
    datetime: str
    customer_id: int
    total: str
    status: str


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--orders",
        "-o",
        dest="orders",
        help="How many orders to create",
        default="6000",
    )
    return parser.parse_args()


def store_csv(orders, sales):
    order_path = "/data/order.csv"
    sales_path = "/data/sales.csv"

    with open(order_path, "w") as f:
        csv_out = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        csv_out.writerow(
            ["id", "order_datetime", "customer_document_number", "total", "status"]
        )
        for o in orders:
            csv_out.writerow(astuple(o))

    with open(sales_path, "w") as f:
        csv_out = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        csv_out.writerow(
            ["order_id", "product_id", "quantity", "price_per_unit", "total"]
        )
        for s in sales:
            csv_out.writerow(astuple(s))


def create_order(order_id, current_month, orders, sales):
    customer_id = random.choice(customer_ids)
    # how many items in the cart
    items = numpy.random.choice([1, 2, 3], p=[0.9, 0.09, 0.01])

    total_order = 0.0
    for _ in range(items.item()):
        idx = random.randint(0, len(product_ids) - 1)
        # how many pieces of the same product, more likely to buy 1,
        # but may also buy 2 or 3
        product_count = numpy.random.choice([1, 2, 3], p=[0.9, 0.09, 0.01])
        product_id = product_ids[idx]
        product_price = prices[idx]
        total_sale = product_price * product_count
        total_order += total_sale
        sales.append(
            Sale(
                order_id,
                product_id,
                int(product_count),
                f"{product_price:.2f}",
                f"{total_sale:.2f}",
            )
        )

    orders.append(
        Order(
            order_id,
            current_month.format("YYYY-MM-DD HH:mm:ss"),
            customer_id,
            f"{total_order:.2f}",
            "COMPLETED",
        )
    )


def main():
    args = parse_args()

    month_delta = pendulum.duration(months=1)
    # Initialize it to 1 year behind
    current_month = pendulum.from_format("2021-08-01", "YYYY-MM-DD")
    orders_per_month = int(int(args.orders) / 12)

    order_id = 1
    orders = []
    sales = []

    # One iteration for every month
    for _ in range(12):
        for _ in range(orders_per_month):
            create_order(order_id, current_month, orders, sales)
            order_id += 1
        current_month += month_delta

    store_csv(orders, sales)


if __name__ == "__main__":
    main()
