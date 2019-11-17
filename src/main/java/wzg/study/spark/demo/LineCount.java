package wzg.study.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class LineCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(LineCount.class.getResource("/data.txt").getPath());
        lines.mapToPair(line -> new Tuple2<>(line, 1))
                .reduceByKey(Integer::sum)
                .foreach(tuple -> System.out.println("line: " + tuple._1 + ", times: " + tuple._2));
    }
}
