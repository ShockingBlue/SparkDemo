package wzg.study.spark.demo.ml;

import lombok.val;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;
import scala.Tuple4;
import wzg.study.spark.demo.ml.bean.LineInfoResult;
import wzg.study.spark.demo.ml.bean.TestLineInfo;
import wzg.study.spark.demo.ml.bean.TrainLineInfo;
import wzg.study.spark.demo.util.SparkUtil;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class WordAnalyze {
    private static Dataset<Row> createDataset(SparkSession session, List<TrainLineInfo> infoList) {
        StructType structType = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.LongType, false),
                DataTypes.createStructField("text", DataTypes.StringType, false),
                DataTypes.createStructField("label", DataTypes.LongType, false)
        });
        List<Row> rowList = infoList.stream()
                .map(obj -> RowFactory.create(obj.getId(), obj.getText(), obj.getLabel()))
                .collect(Collectors.toList());
        return session.createDataFrame(rowList, structType);
    }

    private static Dataset<Row> createDatasetByBean(SparkSession session, List<TrainLineInfo> infoList) {
        return session.createDataFrame(infoList, TrainLineInfo.class);
    }

    private static JavaRDD<TrainLineInfo> createRdd(SparkSession session, List<TrainLineInfo> infoList) {
        JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());
        return jsc.parallelize(infoList);
    }

    public static void main(String[] args) {
        // get spark session
        val session = SparkUtil.getSparkSession();

        // list
        List<TrainLineInfo> trainLineInfoList = Arrays.asList(
                new TrainLineInfo(0L, "a b c d e spark", 1L),
                new TrainLineInfo(1L, "b d", 0L),
                new TrainLineInfo(2L, "spark f g h", 1L),
                new TrainLineInfo(3L, "hadoop mapreduce", 0L));

        // dataset
        Dataset<Row> df = createDatasetByBean(session, trainLineInfoList);
        df.show();

        // make pipeline stages
        val tokenizer = new Tokenizer();
        tokenizer.setInputCol("text");
        tokenizer.setOutputCol("words");
        val tf = new HashingTF();
        tf.setNumFeatures(1000);
        tf.setInputCol(tokenizer.getOutputCol());
        tf.setOutputCol("features");
        val lr = new LogisticRegression();
        lr.setMaxIter(10);
        lr.setRegParam(0.01);

        // make pipeline
        val pipeline = new Pipeline();
        pipeline.setStages(new PipelineStage[]{tokenizer, tf, lr});

        // train
        PipelineModel model = pipeline.fit(df);

        // make test data
        val rawLineInfoList = Arrays.asList(
                new TestLineInfo(4L, "spark i j k"),
                new TestLineInfo(5L, "l m n"),
                new TestLineInfo(6L, "spark a"),
                new TestLineInfo(7L, "apache hadoop"));
        val testDf = session.createDataFrame(rawLineInfoList, TestLineInfo.class);

        // test
        Dataset<Row> result = model.transform(testDf).select(col("id"),
                col("text"),
                col("probability"),
                col("prediction"));
        result.show();
        result.printSchema();

        List<Row> resultList = result.collectAsList();
        resultList.forEach(r -> {
            System.out.println("id=" + r.get(0) + ", text="
                    + r.get(1) + ", probability=" + r.get(2) + ", prediction=" + r.get(3));
        });

        UDF1<DenseVector, String> udf = v->{
            return v.values()[0]+","+v.values()[1];
        };

        session.udf().register("vec2str", udf, DataTypes.StringType);

        val testResult = model.transform(testDf).select(col("id"),
                col("text"),
                functions.callUDF("vec2Str", col("probability").as("probability")),
                col("prediction"))
//                .as(Encoders.bean(LineInfoResult.class))
               .collectAsList();

        testResult.forEach(r->{
            System.out.println("id=" + r.get(0) + ", text="
                    + r.get(1) + ", probability=" + r.get(2) + ", prediction=" + r.get(3));
        });

        session.close();
    }
}
