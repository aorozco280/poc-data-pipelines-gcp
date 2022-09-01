import datetime
import numpy
import random

from faker import Faker
from time import sleep, strftime
from tzlocal import get_localzone

faker = Faker()

LOCAL = get_localzone()
RESPONSES = ["200","404","500"]
VERBS = ["GET","POST","DELETE","PUT"]
RESOURCES = [
    "/login",
    "/search?id=",
    "/cart",
    "/checkout"
]
PLATFORMS = [
    faker.firefox,
    faker.chrome,
    faker.safari
]

def generate_trace():
    now = datetime.datetime.now(LOCAL)
    ip = faker.ipv4()
    date_time = now.strftime('%d/%b/%Y:%H:%M:%S')
    time_zone = now.strftime('%z')
    verb = numpy.random.choice(VERBS, p=[0.75,0.05,0.1,0.1])

    uri = random.choice(RESOURCES)
    if 'search' in uri:
        uri += str(random.randint(100,120))

    resp = numpy.random.choice(RESPONSES, p=[0.9,0.05,0.05])
    byt = int(random.gauss(5000,50))
    referer = faker.uri()
    useragent = numpy.random.choice(PLATFORMS, p=[0.2,0.55,0.25])()

    trace = (
        f"{ip} - - "
        f"[{date_time} {time_zone}] "
        f"\"{verb} {uri} HTTP/1.0\" "
        f"{resp} {byt} "
        f"\"{referer}\" \"{useragent}\""
        "\n"
    )
    return trace

def main():
    traces_to_generate = 1000
    delay = 0.1

    time_str = strftime("%Y%m%d-%H%M%S")
    with open(f"/logs/access_log_{time_str}.log", "w+") as f:
        for _ in range(traces_to_generate):
            trace = generate_trace()
            f.write(trace)
            f.flush()
            sleep(delay)

if __name__ == "__main__":
    main()
