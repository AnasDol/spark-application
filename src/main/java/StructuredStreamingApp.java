import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class StructuredStreamingApp {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("StructuredStreamingApp")
                .master("local[2]") // Use local mode with 2 cores, adjust as needed
                .getOrCreate();

        // Define your structured streaming logic
        Dataset<Row> inputData = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        Dataset<Row> result = inputData.selectExpr("value as input")
                .groupBy("input")
                .count();

        // Start the streaming query
        StreamingQuery query = result.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }
}