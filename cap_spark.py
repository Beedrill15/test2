from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

from datetime import datetime

from pyspark.sql import SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType

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

#creates the context for the stream
sc = SparkContext(appName="StreamingFlume")
sqlc= SQLContext(sc)
rdd = sc.textFile("file:////home/hadoop/spoolDirectory/*.COMPLETED")
schema = StructType([
	StructField("col1_01", StringType())
])
df = sqlc.createDataFrame(rdd, schema=schema)
#sc.setLogLevel("ERROR")
#ssc = StreamingContext(sc, 1)
#spark = SparkSession.builder.appName("Flume").getOrCreate()

#df = spark.read.csv("/home/hadoop/spoolingDirectory/*.COMPLETED")
rdd.saveAsTextFile("hdfs:///surveys/response"+datetime.now().strftime('%Y.%m.%d.%H.%M.%S'))
#creates the stream
#flumeStream = FlumeUtils.createStream(ssc, "localhost", 56565)

#read and save data from stream
#lines = flumeStream.map(lambda x: x[1])
#file = ssc.textFileStream("spoolDirectory/")
#lines = file.flatMap(lambda line: line.split(" ")).map(lambda x: x[1]).reduceByKey(lambda a, b: a + b)
#lines.saveAsTextFiles("hdfs://surveys/response")
#lines.foreach(SaveRecord)

catalog = {
	"table":{"namespace":"default", "name":"surveys"},
	"rowkey":"key",
	"columns":{
		"col0":{"cf":"rowkey", "col":"key", "type":"string"},
		"col1":{"cf":"responses", "col":"col1", "type":"string"},
	}
}
df.write.options(catalog=catalog).format("org.apache.spark.sql.datasources.hbase").save

#ssc.start()
#ssc.awaitTermination()
