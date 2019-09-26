import sys
import time
from pyspark import SparkContext

sc = SparkContext()
file = sc.textFile(sys.argv[1])
wc = file.flatMap(lambda x : x.split(",")).map(lambda x : (x,1)).reduceByKey(lambda x, y : x + y).map(lambda x : (x[0].encode('utf-8') ,x[1] ) ).sortBy(lambda x : (x[1],x[0]))
#wc.coalesce(1).saveAsTextFile(sys.argv[2])

for x in wc.collect() :
    print(x)

sc.stop()
