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


A = sc.parallelize([])

number_of_rows = 0
with open(dataset, 'r') as f:
    for i, line in enumerate(f):
        row = [(i, line_to_float_list(line))]
        A = A.union(sc.parallelize(row))
        number_of_rows += 1

A = A.cache()

AxAt = sc.parallelize([])

with open(dataset, 'r') as f:
    for i, line in enumerate(f):  # TODO: try iterating rdd instead
        row = line_to_float_list(line)
        rdd_row = A.map(
            lambda k_v: (
                k_v[0],
                sum([element * row[i] for i, element in enumerate(k_v[1])])
            )
        )
        rdd_row = rdd_row.values()
        # AxAt = AxAt.union(sc.parallelize([(i, rdd_row.collect())]))
        AxAt = AxAt.union(rdd_row)

# SO FAR SO GOOD, we have AxAt

# Possibility: use toLocalIterator to get row by row for matrix multiplication

import numpy as np
matrix = np.matrix(A.values().collect())
matrix_mul = matrix @ matrix.T
print(matrix_mul)


# r = np.matrix(AxAt.values().collect())
r = AxAt.collect()
print(r)

print(r == matrix_mul)
