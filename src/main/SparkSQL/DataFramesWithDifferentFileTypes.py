from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DFs").getOrCreate()

json = spark.read.json("../resources/people.json")
json.select("name","age").show()