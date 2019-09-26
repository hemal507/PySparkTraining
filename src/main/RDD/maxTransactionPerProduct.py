import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

sc = SparkContext()
txn = sc.textFile(sys.argv[1])
maximum = lambda x,y: max(x,y)

# using RDD
max_txn_rbk = txn.map(lambda x : x.split(",")).map(lambda x: ((x[5],x[4]), int(x[3]))).reduceByKey(maximum).sortByKey()
max_txn_rbk.coalesce(1).saveAsTextFile(sys.argv[2])

for j in max_txn_rbk.collect() :
    print("City - ", j[0][0].encode('utf-8'), " and Product ", j[0][1].encode('utf-8'), " maximum Value is : ", j[1])

# using DataFrames
spark = SparkSession.builder.appName("Max Transaction").getOrCreate()
schema = StructType([StructField("txn_id",      IntegerType(), True),
                    StructField("txn_date",    StringType(),  True),
                    StructField("txn_code",    IntegerType(), True),
                    StructField("txn_amt",     IntegerType(), True),
                    StructField("txn_product", StringType(),  True),
                    StructField("txn_city",    StringType(),  True)]
                    )
df = spark.read.csv(sys.argv[1],schema=schema,header=False)
df.createOrReplaceTempView("transactions")
spark.sql("select txn_city, txn_product, max(txn_amt) from transactions group by txn_city, txn_product")    \
    .coalesce(1).write.mode("overwrite").csv(sys.argv[3])

sc.stop()


'''
max_txn_abk = txn.map(lambda x : x.split(",")).map(lambda x: ((x[5],x[4]), int(x[3]))).aggregateByKey(0, maximum , maximum)
for j in max_txn_abk.collect() :
    print("City -  " , j[0][0].encode('utf-8'), " and Product " , j[0][1].encode('utf-8')  , " maximum Value is : " , j[1])
'''
