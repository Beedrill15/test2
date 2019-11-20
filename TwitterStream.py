#import os
#import findspark
#findspark.init('/usr/hdp/2.5.6.0-40/spark')

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="PythonSparkStreamingkafka")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc,60)

kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter':1})

lines = kafkaStream.map(lambda x: x[1])
lines.pprint()

ssc.start()
ssc.awaitTermination()
