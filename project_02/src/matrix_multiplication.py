from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("danicau").getOrCreate()
dataset = 'random_matrix.txt'
# dataset = 'simple_matrix.txt'
sc = spark.sparkContext


def line_to_float_list(line: str):
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
A = sc.textFile(dataset).map(lambda line: line_to_float_list(line))
A = A.zipWithIndex().map(lambda k_v: (k_v[1], k_v[0]))
A = A.cache()
number_of_rows = A.count()

# Calcualte A x A.T
AxAt = sc.parallelize([])
rows = []
for i in range(number_of_rows):
    row = A.lookup(i)[0]
    rdd_row = A.map(
        lambda k_v: (
            k_v[0],
            sum([element * row[i] for i, element in enumerate(k_v[1])])
        )
    )
    rdd_row = pair_rdd_to_tuple(i, rdd_row)
    rows.append(rdd_row)
    break
AxAt = sc.union(rows).cache()

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
    to_write = rdd_row.collect()
    break
    AxAtxA = AxAtxA.union(rdd_row)


with open('results.txt', 'w') as f:
    for element in to_write[0][1]:
        f.write(f'{element}\n')

# checking
# from checking import check
# check(dataset, AxAtxA.values().collect())
