import datetime
import numpy
import random

from faker import Faker
from time import sleep, strftime
from tzlocal import get_localzone

faker = Faker()

USERS_TO_IPS = [
    (1, "216.145.90.50"),
    (2, "216.145.90.50"),
    (5, "216.145.90.50"),
    (12, "216.145.90.50"),
    (3, "201.148.92.35"),
    (11, "201.148.92.35"),
    (8, "216.155.8.150"),
    (9, "216.155.8.150"),
    (10, "213.216.220.111"),
]
LOCAL = get_localzone()
RESPONSES = ["200", "404", "500"]
VERBS = ["GET", "POST", "PUT"]
RESOURCES = ["/login", "/search?id=", "/cart", "/checkout"]
PLATFORMS = [faker.firefox, faker.chrome, faker.safari]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-records",
        "-m",
        dest="records",
        help="How many records to create",
        default="5000",
    )
    parser.add_argument(
        "--delay",
        "-d",
        dest="delay",
        help="Delay between traces in (float) seconds",
        default="0.1",
    )
    parser.add_argument(
        "--batch-size",
        "-b",
        dest="batch",
        help="Size of every batch of traces",
        default="50",
    )
    return parser.parse_args()


def generate_ip():
    _, ip = numpy.random.choice(
        USERS_TO_IPS, p=[0.3, 0.1, 0.05, 0.05, 0.1, 0.1, 0.1, 0.1, 0.1]
    )
    return ip


def generate_trace():
    now = datetime.datetime.now(LOCAL)
    ip = generate_ip()
    date_time = now.strftime("%d/%b/%Y:%H:%M:%S")
    time_zone = now.strftime("%z")
    verb = numpy.random.choice(VERBS, p=[0.8, 0.1, 0.1])

    uri = random.choice(RESOURCES)
    if "search" in uri:
        uri += str(random.randint(100, 120))

    resp = numpy.random.choice(RESPONSES, p=[0.9, 0.05, 0.05])
    byt = int(random.gauss(5000, 50))
    referer = faker.uri()
    useragent = numpy.random.choice(PLATFORMS, p=[0.2, 0.55, 0.25])()

    trace = (
        f"{ip} - - "
        f"[{date_time} {time_zone}] "
        f'"{verb} {uri} HTTP/1.0" '
        f"{resp} {byt} "
        f'"{referer}" "{useragent}"'
        "\n"
    )
    return trace


def main():
    args = parse_args()
    traces_to_generate = int(args.records)
    batch_size = int(args.batch)
    delay = int(args.batch)

    time_str = strftime("%Y%m%d-%H%M%S")
    with open(f"/apache-logs/access_log_{time_str}.log", "w+") as f:
        batches = int(traces_to_generate / batch_size)
        for i in range(batches):
            for _ in range(batch_size):
                trace = generate_trace()
                f.write(trace)
            sleep(delay)


if __name__ == "__main__":
    main()
