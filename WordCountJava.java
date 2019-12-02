import org.apache.spark.sql.*;
import java.util.*;
import org.apache.spark.api.*;
import java.io.*;
import scala.Tuple2;

public class WordCountJava {
    public static void main(String[] args) {
        SparkConf conf = new Sparkconf().setMaster("local").setAppName("WordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile("shakespeare.txt");
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.saveAsTextFile("JavaOut.txt");
    }
}
