from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

#function to save csv to hbase
def SaveRecord(rdd):
	#host = 'sparkmaster.example.com'
	host = 'localhost'
	table = 'surveys'
	keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
	valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
	conf = {"hbase.zookeeper.quorum": host,
		"hbase.mapred.outputtable": table,
		"mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
		"mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
		"mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
	datamap = rdd.map(lambda x: (str(json.loads(x)["id"]),[str(json.loads(x)["id"]),"sfamily","surveys_json",x]))
	datamap.saveAsNewAPIHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)

sc = SparkContext(appName="StreamingFlume")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)

#flumeStream = FlumeUtils.createStream(ssc, "localhost", 56565)

#lines = flumeStream.map(lambda x: x[1])
file = sc.textFile("/home/william/survey_responses/record_results.csv")
lines = file.map(lambda x: x[1])
lines.foreach(SaveRecord)

ssc.start()
ssc.stop()
# ssc.awaitTermination()
