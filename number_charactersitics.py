from utils import get_pivot
import numpy as np
from pyspark import SparkConf
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Spark").getOrCreate()
conf = SparkConf()
sc = spark.sparkContext

rdd = spark.sparkContext.textFile("numbers_random.txt").map(float)

# print(np.median(rdd.collect()))

minimum = rdd.min()
maximum = rdd.max()
average = rdd.mean()
count = total_count = rdd.count()
variance = rdd.variance()

print(f'minimum: {minimum}')
print(f'maximum: {maximum}')
print(f'average: {average}')
print(f'count: {total_count}')
print(f'variance: {variance}')

left_count = 0
right_count = 0
current_minimum = minimum
current_maximum = maximum
while True:
    if count > 10000:
        sample = rdd.sample(False, 10000 / count)
    else:
        sample = rdd.sample(False, 1)
    step = (current_maximum - current_minimum) / 10
    if current_minimum == current_maximum:
        median = current_minimum
        break
    hist_range = list(
        np.arange(current_minimum, current_maximum + step, step)
    )
    hist = sample.histogram(hist_range)
    a = get_pivot(total_count, left_count, right_count, hist)
    pivot, direction = get_pivot(total_count, left_count, right_count, hist)
    if direction == "left":
        left_rdd = rdd.filter(lambda x: x <= pivot)
        count = left_rdd.count()
        # check if we are finished
        if left_count + count == total_count - left_count - count:
            right_rdd = rdd.filter(lambda x: x >= pivot)
            left = left_rdd.max()
            right = right_rdd.min()
            median = (left + right) / 2
            break
        elif left_count + count == total_count - left_count - count + 1:
            median = left_rdd.max()
            break
        elif left_count + count == total_count - left_count - 1:
            right_rdd = rdd.filter(lambda x: x >= pivot)
            median = right_rdd.min()
            break
        # check we have the correct side rdd, or if median is on the other side
        elif left_count + count > total_count / 2:
            rdd = left_rdd.cache()
            right_count = total_count - left_count - count
            current_maximum = rdd.max()
        else:
            rdd = rdd.filter(lambda x: x >= pivot).cache()
            left_count += count
            current_minimum = rdd.min()
    elif direction == "right":
        right_rdd = rdd.filter(lambda x: x >= pivot).cache()
        count = right_rdd.count()
        # check if we are finished
        if right_count + count == total_count - right_count - count:
            left_rdd = rdd.filter(lambda x: x <= pivot)
            right = right_rdd.min()
            left = left_rdd.max()
            median = (right + left) / 2
            break
        elif right_count + count == total_count - right_count - count + 1:
            median = right_rdd.min()
            break
        elif right_count + count == total_count - right_count - 1:
            left_rdd = rdd.filter(lambda x: x <= pivot)
            median = left_rdd.max()
            break
        # check we have the correct side rdd, or if median is on the other side
        elif right_count + count > total_count / 2:
            rdd = right_rdd.cache()
            left_count = total_count - right_count - count
            current_minimum = rdd.min()
        else:
            rdd = rdd.filter(lambda x: x <= pivot).cache()
            right_count += count
            current_maximum = right_rdd.max()

print(f'median: {median}')
