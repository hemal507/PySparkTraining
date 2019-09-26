import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount",master="local[2]")
    ssc = StreamingContext(sc, 2)
    brokers="localhost:9092"
    topic = "twitter_topic"
    kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    lines.pprint()
    ssc.start()
    ssc.awaitTermination()