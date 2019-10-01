from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.appName("Socket strucutred stream").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions",2)
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

words = lines.select(explode(split(lines.value, " ")).alias("word"))

wcounts = words.groupBy("word").count()

query = wcounts.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()