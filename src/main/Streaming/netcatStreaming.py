from pyspark import SparkContext
from pyspark.streaming import StreamingContext

#  nc -lk 9999

sc = SparkContext()
ssc = StreamingContext(sc,10)

netcatDStream = ssc.socketTextStream("localhost",9999)
words = netcatDStream.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda x, y: sum(x, y))
words.pprint()

ssc.start()
ssc.awaitTermination()
