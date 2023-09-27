import random
import argparse

from pyspark import SparkContext

NUM_SAMPLES = 1000

sc = SparkContext()

def inside(p):
    x, y = random.random(), random.random()
    return x * x + y * y < 1


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-n", default=NUM_SAMPLES)

    args = parser.parse_args()

    n = int(args.n)

    print(f"Using {n} points")

    count = sc.parallelize(range(0, n)).filter(inside).count()

    print("Pi is roughly %f" % (4.0 * count / n))
