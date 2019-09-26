from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def updatefunc(current, previous):
    if previous is None:
        previous=0
    if current is None :
        return previous
    return sum(current) + previous


sc = SparkContext()
ssc = StreamingContext(sc,10)
ssc.checkpoint("/usr/local/spark/sbk1")

stream =ssc.socketTextStream("localhost",9999)
counts = stream.flatMap(lambda x : x.split(" ")).map(lambda x : (x, 1)).updateStateByKey(updatefunc)
counts.pprint()

ssc.start()
ssc.awaitTermination()