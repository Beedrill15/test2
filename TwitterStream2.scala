import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

import java.io.File
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql
import org.apache.spark._
import org.apache.hadoop.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

//import org.apache.spark.rdd.NewHadoopRDD
//import org.apache.hadoop.hbase.{HbaseConfiguration, HTableDescriptor}
//import org.apache.hadoop.hbase.client.HBaseAdmin
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HColumnDescriptor
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.HTable;

object TwitterStream2 {
	def main(args: Array[String]) {
		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> "localhost:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "use_a_separate_group_id_for_each_stream",
			"auto.offset.reset" -> "latest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)

		print("I have set the parameters")

		val warehouseLocation = new File("/user/hive/warehouse").getAbsolutePath
		val spark = SparkSession.builder().appName("tweeter").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()

		print("I have established the SparkSession")

		import spark.implicits._
		
		import spark.sql

		val ssc = new StreamingContext(spark.sparkContext, Seconds(2))

		print("I have made the StreamingContext")

		val topics = Array("twitter")
		val stream = KafkaUtils.createDirectStream[String, String](
			ssc,
			PreferConsistent,
			Subscribe[String, String](topics, kafkaParams)
		)

		print("I have made the stream")

		stream.foreachRDD { rdd =>
			rdd.foreach { record =>
				val value = record.value()
				val tweet = scala.util.parsing.json.JSON.parseFull(value)
				val map:Map[String,Any] = tweet.get.asInstanceOf[Map[String, Any]]
				println(map.get("text"))
			}
		}

//		val conf = HBaseConfiguration.create()
//		val tableName = "tweets"
//		conf.set(TableInputFormat.INPUT_TABLE, tableName)

//		val myTable = new HTable(conf, tableName);
//		var p = new Put();
//		p = new Put(new String("row999").getBytes());
//		p.add("cf".getBytes(), "column_name".getBytes(), new String("value999").getBytes());
//		myTable.put(p);
//		myTable.flushCommits();
		ssc.start()
		ssc.awaitTermination()
	}
}
