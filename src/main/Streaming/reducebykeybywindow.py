from pyspark.streaming import StreamingContext
from pyspark import SparkContext

sc = SparkContext()
ssc = StreamingContext(sc, 10)
ssc.checkpoint("/usr/local/spark/pspc1")

Dstr = ssc.socketTextStream("localhost", 9999)
words = Dstr.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1))   \
    .reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 10)

words.pprint()

ssc.start()
ssc.awaitTermination()