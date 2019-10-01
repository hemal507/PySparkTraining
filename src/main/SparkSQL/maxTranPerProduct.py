import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

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

df2.explain()

spark.stop()
