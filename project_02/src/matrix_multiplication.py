from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("danicau").getOrCreate()
dataset = 'random_matrix.txt'
dataset = 'simple_matrix.txt'
sc = spark.sparkContext


def line_to_float_list(line: str):
    line = line[:-1]  # remove \n
    line = line.split()
    line = [float(x) for x in line]
    return line


def pair_rdd_to_same_key(key, pair_rdd):
    return pair_rdd.map(lambda k_v: (key, k_v[1]))


def pair_rdd_to_tuple(key, pair_rdd):
    return pair_rdd_to_same_key(key, pair_rdd).groupByKey().mapValues(list)


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
A_iterator = A.values().toLocalIterator()
for i, row in enumerate(A_iterator):
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
AxAt_iterator = AxAt.values().toLocalIterator()
for i, row in enumerate(AxAt_iterator):
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

# checking code
# import numpy as np
# matrix = np.matrix(A.values().collect())
# matrix_mul = matrix @ matrix.T
# r = AxAt.values().collect()
# print(np.all(r == matrix_mul))

# matrix_mul = matrix @ matrix.T @ matrix
# r = AxAtxA.values().collect()
# print(np.all(r == matrix_mul))
