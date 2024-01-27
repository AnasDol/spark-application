import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import org.apache.spark.sql.streaming.StreamingQueryException;
import java.util.concurrent.TimeoutException;

import java.util.Arrays;

public class StructuredStreamingApp {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        // Initialize SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredNetworkWordCount")
                //.master("local[2]") // Use local mode with 2 cores, adjust as needed
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
//        Dataset<Row> lines = spark
//                .readStream()
//                .format("socket")
//                .option("host", "localhost")
//                .option("port", 9999)
//                .load();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "test2")
                .load();

        // Split the lines into words
//        Dataset<String> words = lines
//                .as(Encoders.STRING())
//                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
         StreamingQuery query = df
                 .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                 .writeStream()
                 .outputMode("append")
                 .format("console")
                 .start();

        // Generate running word count
//        Dataset<Row> wordCounts = words.groupBy("value").count();



//        Dataset<Row> result = inputData.selectExpr("value as input")
//                .groupBy("input")
//                .count();


        // Start running the query that prints the running counts to the console
//        StreamingQuery query = wordCounts.writeStream()
//                .outputMode("complete")
//                .format("console")
//                .start();

        query.awaitTermination();

    }
}