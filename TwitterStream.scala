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
		val sparkConf = new SparkConf().setAppName("tweeter")
		val ssc = new StreamingContext(sparkConf, Seconds(2))

		val warehouseLocation = new File("hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/tweets").getAbsolutePath
		val spark = SparkSession.builder().appName("tweeter").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
		import spark.implicits._
		
		import spark.sql

		val topics = Array("twitter")
		val stream = KafkaUtils.createDirectStream[String, String](
			ssc,
			PreferConsistent,
			Subscribe[String, String](topics, kafkaParams)
		)

		def change(into: RDD[String]) : DataFrame = {
			val outof = into.toDF()
			return outof
		}

		sql("DROP TABLE IF EXISTS tweets")

		stream.foreachRDD { rdd =>
			val dataFrame = rdd.map(row => row.value()).toDF().coalesce(1)
			dataFrame.write.mode(SaveMode.Append).saveAsTable("tweets")
			rdd.foreach { record =>
				val value = record.value()
				val tweet = scala.util.parsing.json.JSON.parseFull(value)
				val map:Map[String,Any] = tweet.get.asInstanceOf[Map[String, Any]]
				println(map.get("text"))

//				val df = Seq(map.get("text").toString).toDF()
//				df.write.mode(SaveMode.Append).saveAsTable("tweets")
			}
		}
		
		ssc.start()
		ssc.awaitTermination()
	}
}
