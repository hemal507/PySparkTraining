from pyspark import SparkContext

sc = SparkContext()
rdd1 = sc.textFile("resources/transactions.txt").map(lambda x : x.split(","))   \
            .map(lambda x : (x[2] , int(x[3]))).reduceByKey(lambda x, y: max(x, y))
rdd2 = sc.textFile("resources/users").map(lambda x : x.split(","))  \
            .map(lambda x : (x[0],x[1]))

rdd1.join(rdd2).coalesce(1).saveAsTextFile("resources/rddJoin")

