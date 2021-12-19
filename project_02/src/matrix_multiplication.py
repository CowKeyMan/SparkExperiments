# running on server
from pyspark import SparkContext, SparkConf
dataset = "/wrk/group/grp-ddi-2021/datasets/data-2.txt"
conf = (
    SparkConf()
    .setAppName("danicau")
    .setMaster("spark://ukko2-10.local.cs.helsinki.fi:7077")
    .set("spark.cores.max", "10")
    .set("spark.rdd.compress", "true")
    .set("spark.broadcast.compress", "true")
)
sc = SparkContext(conf=conf)

# # local testing
# from pyspark import SparkConf
# from pyspark.sql import SparkSession
# dataset = 'simple_matrix.txt'
# spark = SparkSession.builder.appName("Spark SQL basic example").getOrCreate()
# conf = SparkConf()
# sc = spark.sparkContext


def line_to_float_list(line: str):
    line = line.split()
    line = [float(x) for x in line]
    return line


# Get A
A = sc.textFile(dataset).map(lambda line: line_to_float_list(line))
A = A.zipWithIndex().map(lambda k_v: (k_v[1], k_v[0]))
A = A.cache()
number_of_rows = A.count()

with open('results.txt', 'w') as f:
    for i in range(number_of_rows):
        # Calcualte row for A x A.T
        A_row = A.lookup(i)[0]
        AAT_rdd_row = A.map(
            lambda k_v: (
                k_v[0],
                sum([element * A_row[i] for i, element in enumerate(k_v[1])])
            )
        )
        AAT_row = sorted(AAT_rdd_row.collect(), key=lambda x: x[0])
        AAT_row = [element[1] for element in AAT_row]
        # calculate row for A x A.T x A
        AATA_rdd_row = A.map(
            lambda k_v: (
                k_v[0],
                [element * AAT_row[k_v[0]] for element in k_v[1]]
            )
        )
        AATA_row = AATA_rdd_row.reduce(
            lambda l1, l2: (i, [a + b for a, b in zip(l1[1], l2[1])])
        )
        # write row by row
        f.write(f'{" ".join([str(elem) for elem in AATA_row[1]])}\n')
        # write each element to its own line
        # f.write("\n".join([str(elem) for elem in AATA_row[1]]) + '\n')
        break


# # checking
# from checking import check, file_to_matrix
# check(dataset, file_to_matrix('results.txt'))
