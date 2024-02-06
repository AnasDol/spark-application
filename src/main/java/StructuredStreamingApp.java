import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import org.apache.spark.sql.streaming.StreamingQueryException;
import java.util.concurrent.TimeoutException;
import static org.apache.spark.sql.avro.functions.*;
import static org.apache.spark.sql.functions.*;

import java.util.Arrays;

public class StructuredStreamingApp {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();

        Dataset<Row> kafkaDF = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "test2")
                .load();

        Dataset<Row> df = kafkaDF
                .selectExpr("CAST(value AS STRING)")
                .select(split(new Column("value"), ",").alias("csv_values"))
                .selectExpr("csv_values[0] AS ip", "CAST(csv_values[1] AS INT) AS num");

        Dataset<Row> jdbcDF = spark
                .read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://host.docker.internal:5432/diploma")
                .option("dbtable", "public.table1")
                .option("user", "postgres")
                .option("password", "7844")
                .load();

        // Соединяем таблицы по ip и заполняем недостающие numы
        Dataset<Row> enrichedDF_num = df.join(jdbcDF, df.col("ip").equalTo(jdbcDF.col("ip")), "left_outer")
                .select(
                        df.col("ip"),
                        df.col("num"),
                        jdbcDF.col("ip").alias("jdbcIp"),
                        jdbcDF.col("num").alias("jdbcNum"),
                        when(df.col("num").isNull(), jdbcDF.col("num").alias("smartNum"))
                                .otherwise(df.col("num")).alias("smartNum")
                );

        // Соединяем по num и заполняем ip
        Dataset<Row> enrichedDF_ip = enrichedDF_num.join(jdbcDF, enrichedDF_num.col("num").equalTo(jdbcDF.col("num")), "left_outer")
                .select(
                        when(enrichedDF_num.col("ip").isNull(), jdbcDF.col("ip"))
                                .otherwise(enrichedDF_num.col("ip")).alias("ip"),
                        enrichedDF_num.col("num")
                );

        StreamingQuery query = enrichedDF_num
                .writeStream()
                .outputMode("append")
                .format("console")
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

    }
}