import math
from utils import get_pivot
from pyspark import SparkConf
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Spark").getOrCreate()
conf = SparkConf()
sc = spark.sparkContext

rdd = spark.sparkContext.textFile("numbers_random.txt").map(float)

minimum = rdd.min()
maximum = rdd.max()
average = rdd.mean()
count = total_count = rdd.count()
variance = rdd.variance()
sd = math.sqrt(variance)

print(f'minimum: {minimum}')
print(f'maximum: {maximum}')
print(f'average: {average}')
print(f'count: {total_count}')
print(f'variance: {variance}')

right_rdd = rdd.filter(lambda x: x >= average - sd)
left_count = count - right_rdd.count()
left_rdd = right_rdd.filter(lambda x: x <= average + sd)
right_count = count - left_rdd.count() - left_count
rdd = left_rdd

initial_partitions = rdd.getNumPartitions()
current_minimum = rdd.min()
current_maximum = rdd.max()
while True:
    sample = rdd.sample(False, min(10000 / count, 1))
    if current_minimum == current_maximum:
        median = current_minimum
        break
    step = (current_maximum - current_minimum) / 10
    hist_range = [current_minimum + x * step for x in range(0, 11)]
    hist_range[-1] = current_maximum
    hist = sample.histogram(hist_range)
    a = get_pivot(total_count, left_count, right_count, hist)
    pivot, direction = get_pivot(total_count, left_count, right_count, hist)
    if direction == "left":
        left_rdd = rdd.filter(lambda x: x <= pivot)
        count = left_rdd.count()
        # check if we are finished
        if left_count + count == total_count - left_count - count:
            right_rdd = rdd.filter(lambda x: x > pivot)
            left = left_rdd.max()
            right = right_rdd.min()
            median = (left + right) / 2
            break
        elif left_count + count == total_count - left_count - count + 1:
            median = left_rdd.max()
            break
        elif left_count + count == total_count - left_count - count - 1:
            right_rdd = rdd.filter(lambda x: x > pivot)
            median = right_rdd.min()
            break
        # check we have the correct side rdd, or if median is on the other side
        elif left_count + count > total_count / 2:
            rdd = left_rdd.coalesce(
                math.ceil(initial_partitions * (count / total_count))
            ).cache()
            right_count = total_count - left_count - count
            current_maximum = rdd.max()
        else:
            rdd = rdd.filter(lambda x: x > pivot).coalesce(
                math.ceil(initial_partitions * (count / total_count))
            ).cache()
            left_count += count
            current_minimum = rdd.min()
    elif direction == "right":
        right_rdd = rdd.filter(lambda x: x > pivot)
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
        elif right_count + count == total_count - right_count - count - 1:
            left_rdd = rdd.filter(lambda x: x <= pivot)
            median = left_rdd.max()
            break
        # check we have the correct side rdd, or if median is on the other side
        elif right_count + count > total_count / 2:
            rdd = right_rdd.coalesce(
                math.ceil(initial_partitions * (count / total_count))
            ).cache()
            left_count = total_count - right_count - count
            current_minimum = rdd.min()
        else:
            rdd = rdd.filter(lambda x: x <= pivot).coalesce(
                math.ceil(initial_partitions * (count / total_count))
            ).cache()
            right_count += count
            current_maximum = right_rdd.max()

print(f'median: {median}')
