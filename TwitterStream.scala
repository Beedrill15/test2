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

object TwitterStream {
	def main(args: Array[String]) {
		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> "localhost:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "use_a_separate_group_id_for_each_stream",
			"auto.offset.reset" -> "latest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)

		println("I have set the parameters")

		val warehouseLocation = "/apps/hive/warehouse"
		val spark = SparkSession.builder().appName("tweeter").config("spark.sql.warehouse.dir", warehouseLocation).config("hive.metastore.uris", "thrift://localhost:9083").enableHiveSupport().getOrCreate()

		println("I have established the SparkSession")

		import spark.implicits._
		
		import spark.sql

		val ssc = new StreamingContext(spark.sparkContext, Seconds(2))

		println("I have made the StreamingContext")

		val topics = Array("twitter")
		val stream = KafkaUtils.createDirectStream[String, String](
			ssc,
			PreferConsistent,
			Subscribe[String, String](topics, kafkaParams)
		)

		println("I have made the stream")

		sql("DROP TABLE IF EXISTS tweets").show()
//		sql("CREATE TABLE tweets(text String)").show()
		println("I should have created the table")

		stream.foreachRDD { rdd =>
			val dataFrame = rdd.map(row => row.value()).toDF().coalesce(1)
			dataFrame.write.mode(SaveMode.Append).saveAsTable("tweets")

			rdd.foreach { record =>
				val value = record.value()
				val tweet = scala.util.parsing.json.JSON.parseFull(value)
				val map:Map[String,Any] = tweet.get.asInstanceOf[Map[String, Any]]
				println(map.get("text"))
			}
		}
		
		ssc.start()
		ssc.awaitTermination()
	}
}
