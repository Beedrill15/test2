import org.apache.spark.sql.*
import org.apache.spark.api.*
import org.apache.spark.{Sparkconf, SparkContext}
import java.io._

object WordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new Sparkcontext(conf)
    val textFile = sc.textFile("shakespeare.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("ScalaOut.txt")
  }
}
