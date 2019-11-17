package wzg.study.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TransformDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TransformDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<>("class1", 88),
                new Tuple2<>("class2", 85),
                new Tuple2<>("class2", 80),
                new Tuple2<>("class1", 75));
        JavaPairRDD<String, Integer> pairRdd = sc.parallelizePairs(list);
        pairRdd.groupByKey().foreach(tp->{
            System.out.print(tp._1 + ": ");
            tp._2.forEach(value -> System.out.print(value+" "));
            System.out.println();
        });
        sc.close();
    }
}
