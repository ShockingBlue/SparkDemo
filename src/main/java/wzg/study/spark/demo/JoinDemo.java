package wzg.study.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class JoinDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TransformDemo").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> list1 = Arrays.asList(
                new Tuple2<>("class1", 1),
                new Tuple2<>("class2", 2),
                new Tuple2<>("class3", 3),
                new Tuple2<>("class4", 4));
        List<Tuple2<String, Integer>> list2 = Arrays.asList(
                new Tuple2<>("class1", 11),
                new Tuple2<>("class1", 111),
                new Tuple2<>("class3", 33),
                new Tuple2<>("class4", 44));
        JavaPairRDD<String, Integer> pairRdd1 = sc.parallelizePairs(list1);
        JavaPairRDD<String, Integer> pairRdd2 = sc.parallelizePairs(list2);

        pairRdd1.join(pairRdd2).foreach(tp -> {
            System.out.println("join: " + tp._1 + ", " + tp._2);
        });
        pairRdd1.cogroup(pairRdd2).foreach(tp -> {
            System.out.println("cogroup: " + tp._1 + ", " + tp._2);
        });
        sc.close();
    }

}
