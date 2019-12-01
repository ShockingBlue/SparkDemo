package wzg.study.spark.demo.util;

import org.apache.spark.sql.SparkSession;

public class SparkUtil {
    private SparkUtil() {
    }

    public static SparkSession getSparkSession() {
        return SparkSession.builder().appName("spark-demo").master("local[*]").getOrCreate();
    }
}
