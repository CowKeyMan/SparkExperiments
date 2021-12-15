from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("danicau").getOrCreate()
dataset = 'random_matrix.txt'
# dataset = 'simple_matrix.txt'
sc = spark.sparkContext


def line_to_float_list(line: str):
    line = line[:-1]  # remove \n
    line = line.split()
    line = [float(x) for x in line]
    return line


def pair_rdd_to_same_key(key, pair_rdd):
    return pair_rdd.map(lambda k_v: (key, k_v[1]))


def pair_rdd_to_tuple(key, pair_rdd):
    # The following may be necessary in case groupByKey does not retain order
    lst = pair_rdd_to_same_key(key, pair_rdd).values().collect()
    return sc.parallelize([(key, lst)])


# Get A
A = sc.parallelize([])
number_of_rows = 0
with open(dataset, 'r') as f:
    for i, line in enumerate(f):
        row = [(i, line_to_float_list(line))]
        A = A.union(sc.parallelize(row))
        number_of_rows += 1
A = A.cache()

# Calcualte A x A.T
AxAt = sc.parallelize([])
for i in range(number_of_rows):
    row = A.lookup(i)[0]
    rdd_row = A.map(
        lambda k_v: (
            k_v[0],
            sum([element * row[i] for i, element in enumerate(k_v[1])])
        )
    )
    rdd_row = pair_rdd_to_tuple(i, rdd_row)
    AxAt = AxAt.union(rdd_row)
AxAt = AxAt.cache()

# Calcualte A x A.T x A
AxAtxA = sc.parallelize([])
for i in range(number_of_rows):
    row = AxAt.lookup(i)[0]
    rdd_row = A.map(
        lambda k_v: (
            k_v[0],
            [element * row[k_v[0]] for element in k_v[1]]
        )
    )
    rdd_row = pair_rdd_to_same_key(i, rdd_row)
    rdd_row = rdd_row.reduceByKey(
        lambda l1, l2:
        [a + b for a, b in zip(l1, l2)]
    )
    AxAtxA = AxAtxA.union(rdd_row)

# checking
# from checking import check
# check(dataset, AxAtxA.values().collect())
