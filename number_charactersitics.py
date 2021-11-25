import math
from utils import get_pivot
from pyspark import SparkContext, SparkConf


dataset = "/wrk/group/grp-ddi-2021/datasets/data-1.txt"
conf = (
    SparkConf()
    .setAppName("danicau")
    .setMaster("spark://ukko2-10.local.cs.helsinki.fi:7077")
    .set("spark.cores.max", "10")
    .set("spark.rdd.compress", "true")
    .set("spark.broadcast.compress", "true")
)
sc = SparkContext(conf=conf)

rdd = sc.textFile(dataset).map(float).cache()

minimum = rdd.min()
print(f'minimum: {minimum}')

maximum = rdd.max()
print(f'maximum: {maximum}')

average = rdd.mean()
print(f'average: {average}')

count = total_count = rdd.count()
print(f'count: {total_count}')

variance = rdd.variance()
print(f'variance: {variance}')

# median calculation
sd = math.sqrt(variance)

right_rdd = rdd.filter(lambda x: x >= average - sd)
left_count = count - right_rdd.count()
left_rdd = right_rdd.filter(lambda x: x <= average + sd)
right_count = count - left_rdd.count() - left_count
rdd = left_rdd

current_partitions = initial_partitions = rdd.getNumPartitions()
current_minimum = rdd.min()
current_maximum = rdd.max()
while True:
    new_partitions = math.ceil(initial_partitions * (count / total_count))
    if current_partitions >= new_partitions * 10:
        rdd = rdd.repartition(new_partitions).cache()
        current_partitions = new_partitions
    if current_minimum == current_maximum:
        median = current_minimum
        break
    step = (current_maximum - current_minimum) / 10
    hist_range = [current_minimum + x * step for x in range(0, 11)]
    hist_range[-1] = current_maximum
    hist = rdd.histogram(hist_range)
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
            rdd = left_rdd.cache()
            right_count = total_count - left_count - count
            current_maximum = rdd.max()
        else:
            rdd = rdd.filter(lambda x: x > pivot).cache()
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
            rdd = right_rdd.cache()
            left_count = total_count - right_count - count
            current_minimum = rdd.min()
        else:
            rdd = rdd.filter(lambda x: x <= pivot).cache()
            right_count += count
            current_maximum = right_rdd.max()

print(f'median: {median}')
