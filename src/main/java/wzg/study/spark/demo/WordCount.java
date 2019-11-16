package wzg.study.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        // set config param
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[2]");

        // make context object, JavaSparkContext for java
        JavaSparkContext sc = new JavaSparkContext(conf);

        // make RDD from text file
        String path = WordCount.class.getResource("/data.txt").getPath();
        JavaRDD<String> lines = sc.textFile(path);

        // action to print, be careful the lambda must be serializable
        lines.foreach(line -> System.out.println(line));

        // transformation and action
        lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .foreach(tuple -> System.out.println(tuple._1 + ": " + tuple._2));
    }
}
