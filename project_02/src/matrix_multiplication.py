# # running on server
# from pyspark import SparkContext, SparkConf
# dataset = "/wrk/group/grp-ddi-2021/datasets/data-2.txt"
# conf = (
#     SparkConf()
#     .setAppName("danicau")
#     .setMaster("spark://ukko2-10.local.cs.helsinki.fi:7077")
#     .set("spark.cores.max", "10")
#     .set("spark.rdd.compress", "true")
#     .set("spark.broadcast.compress", "true")
# )
# sc = SparkContext(conf=conf)

# local testing
from pyspark import SparkConf
from pyspark.sql import SparkSession
dataset = 'simple_matrix.txt'
spark = SparkSession.builder.appName("Spark SQL basic example").getOrCreate()
conf = SparkConf()
sc = spark.sparkContext




def line_to_float_list(line: str):
    line = line.split()
    line = [float(x) for x in line]
    return line


def pair_rdd_to_same_key(key, pair_rdd):
    return pair_rdd.map(lambda k_v: (key, k_v[1]))


def pair_rdd_to_tuple(key, pair_rdd):
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

# Calcualte A x A.T x A and write to file
with open('results.txt', 'w') as f:
    for i in range(number_of_rows):
        row = AxAt.lookup(i)[0]
        rdd_row = A.map(
            lambda k_v: (
                k_v[0],
                [element * row[k_v[0]] for element in k_v[1]]
            )
        )
        rdd_row = rdd_row.reduce(
            lambda l1, l2: (i, [a + b for a, b in zip(l1[1], l2[1])])
        )
        # write row by row
        # f.write(f'{" ".join([str(elem) for elem in rdd_row[1]])}\n')
        # write each element to its own line
        f.write("\n".join([str(elem) for elem in rdd_row[1]]) + '\n')
        break


# checking
# from checking import check, file_to_matrix
# check(dataset, file_to_matrix('results.txt'))
