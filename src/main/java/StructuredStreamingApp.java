import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class StructuredStreamingApp {

    public static void main(String[] args) throws TimeoutException {

        SparkSession spark = SparkSession
                .builder()
                .appName("spark")
                .getOrCreate();

        Dataset<Row> srcDF = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "test3")
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> splitted = srcDF
                .select(col("value").cast("string"))
                .select(split(col("value"), ",").alias("csv_values"))
                .select(
                        col("csv_values").getItem(0).cast("timestamp").as("start_time"),
                        col("csv_values").getItem(1).cast("string").as("measuring_probe_name"),
                        col("csv_values").getItem(2).cast("long").as("imsi"),
                        col("csv_values").getItem(3).cast("long").as("msisdn"),
                        col("csv_values").getItem(4).cast("string").as("ms_ip_address")
//                        col("csv_values").getItem(0).cast("string").as("measuring_probe_name"),
//                        col("csv_values").getItem(1).cast("long").as("start_time"),
//                        col("csv_values").getItem(2).cast("long").as("procedure_duration"),
//                        col("csv_values").getItem(3).cast("string").as("subscriber_activity"),
//                        col("csv_values").getItem(4).cast("long").as("procedure_type"),
//                        col("csv_values").getItem(5).cast("long").as("imsi"),
//                        col("csv_values").getItem(6).cast("long").as("msisdn"),
//                        col("csv_values").getItem(7).cast("string").as("apn"),
//                        col("csv_values").getItem(8).cast("string").as("uri"),
//                        col("csv_values").getItem(9).cast("string").as("ms_ip_address"),
//                        col("csv_values").getItem(10).cast("long").as("total_data_ul"),
//                        col("csv_values").getItem(11).cast("long").as("total_data_dl"),
//                        col("csv_values").getItem(12).cast("long").as("mcc"),
//                        col("csv_values").getItem(13).cast("long").as("mnc"),
//                        col("csv_values").getItem(14).cast("string").as("imei"),
//                        col("csv_values").getItem(15).cast("long").as("cgi"),
//                        col("csv_values").getItem(16).cast("long").as("rat"),
//                        col("csv_values").getItem(17).cast("string").as("host"),
//                        col("csv_values").getItem(18).cast("long").as("tai_tac"),
//                        col("csv_values").getItem(19).cast("long").as("lac"),
//                        col("csv_values").getItem(20).cast("long").as("sac"),
//                        col("csv_values").getItem(21).cast("long").as("cell_id"),
//                        col("csv_values").getItem(22).cast("long").as("enb_id"),
//                        col("csv_values").getItem(23).cast("long").as("event_type"),
//                        col("csv_values").getItem(24).cast("string").as("source_ip"),
//                        col("csv_values").getItem(25).cast("string").as("dest_ip"),
//                        col("csv_values").getItem(26).cast("long").as("effective_throughput_dl"),
//                        col("csv_values").getItem(27).cast("string").as("roaming_operator_name"),
//                        col("csv_values").getItem(28).cast("string").as("roaming_direction"),
//                        col("csv_values").getItem(29).cast("long").as("roaming_status"),
//                        col("csv_values").getItem(30).cast("long").as("rtt"),
//                        col("csv_values").getItem(31).cast("double").as("internet_latency"),
//                        col("csv_values").getItem(32).cast("double").as("ran_latency"),
//                        col("csv_values").getItem(33).cast("string").as("charging_type_name"),
//                        col("csv_values").getItem(34).cast("string").as("protocol"),
//                        col("csv_values").getItem(35).cast("long").as("release_cause"),
//                        col("csv_values").getItem(36).cast("long").as("ttfb"),
//                        col("csv_values").getItem(37).cast("long").as("mean_throughput_ul"),
//                        col("csv_values").getItem(38).cast("long").as("mean_throughput_dl"),
//                        col("csv_values").getItem(39).cast("long").as("peak_throughput_ul"),
//                        col("csv_values").getItem(40).cast("long").as("peak_throughput_dl"),
//                        col("csv_values").getItem(41).cast("long").as("effective_throughput_ul"),
//                        col("csv_values").getItem(42).cast("long").as("unique_cdr_id"),
//                        col("csv_values").getItem(43).cast("long").as("total_dl_raw_bytes"),
//                        col("csv_values").getItem(44).cast("long").as("total_ul_raw_bytes"),
//                        col("csv_values").getItem(45).cast("long").as("total_dl_tcp_bytes_no_retrans"),
//                        col("csv_values").getItem(46).cast("long").as("total_ul_tcp_bytes_no_retrans"),
//                        col("csv_values").getItem(47).cast("string").as("cell_region"),
//                        col("csv_values").getItem(48).cast("double").as("tcp_retrans_frames_ratio_dl"),
//                        col("csv_values").getItem(49).cast("double").as("tcp_retrans_frames_ratio_ul"),
//                        col("csv_values").getItem(50).cast("long").as("total_dl_tcp_frames"),
//                        col("csv_values").getItem(51).cast("long").as("total_ul_tcp_frames"),
//                        col("csv_values").getItem(52).cast("long").as("total_dl_tcp_bytes"),
//                        col("csv_values").getItem(53).cast("long").as("total_ul_tcp_bytes"),
//                        col("csv_values").getItem(54).cast("double").as("tcp_retrans_bytes_ratio_dl"),
//                        col("csv_values").getItem(55).cast("double").as("tcp_retrans_bytes_ratio_ul"),
//                        col("csv_values").getItem(56).cast("long").as("total_ul_frames"),
//                        col("csv_values").getItem(57).cast("long").as("total_dl_frames"),
//                        col("csv_values").getItem(58).cast("string").as("application_name"),
//                        col("csv_values").getItem(59).cast("string").as("rat_type_name"),
//                        col("csv_values").getItem(60).cast("long").as("estimate_minimal_playout_time"),
//                        col("csv_values").getItem(61).cast("long").as("video_total_duration"),
//                        col("csv_values").getItem(62).cast("long").as("video_rebuffer_count"),
//                        col("csv_values").getItem(63).cast("long").as("video_rebuffer_duration"),
//                        col("csv_values").getItem(64).cast("long").as("video_resolution_duration_1"),
//                        col("csv_values").getItem(65).cast("long").as("video_resolution_duration_2"),
//                        col("csv_values").getItem(66).cast("long").as("video_resolution_duration_3"),
//                        col("csv_values").getItem(67).cast("long").as("video_resolution_duration_4"),
//                        col("csv_values").getItem(68).cast("double").as("avg_playout_vmos_score"),
//                        col("csv_values").getItem(69).cast("long").as("total_dl_video_bytes"),
//                        col("csv_values").getItem(70).cast("string").as("service_src_ip"),
//                        col("csv_values").getItem(71).cast("string").as("service_dst_ip"),
//                        col("csv_values").getItem(72).cast("string").as("src_ip_timestamps_list"),
//                        col("csv_values").getItem(73).cast("string").as("dest_ip_timestamps_list"),
//                        col("csv_values").getItem(74).cast("string").as("filter")
                );


        Dataset<Row> withStartAndProbe = splitted
                .withColumn("event_date", functions.to_date(col("start_time")))
                .withColumn("probe", functions.substring(col("measuring_probe_name"), 1, 2));

        String[] columnNames = withStartAndProbe.columns();

        Dataset<Row> exploded_input = withStartAndProbe
                .withColumn("ip", functions.explode(functions.split(trim(col("ms_ip_address")), ";")))
                .withColumn("ip", trim(col("ip")))
                .filter(col("ip").notEqual(""));


        Dataset<Row> imsi_msisdn = spark
                .read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://host.docker.internal:5432/diploma")
                .option("dbtable", "public.imsi_msisdn")
                .option("user", "postgres")
                .option("password", "7844")
                .load()
                .withColumnRenamed("imsi", "_imsi")
                .withColumnRenamed("msisdn", "_msisdn");

        imsi_msisdn.persist(StorageLevel.MEMORY_AND_DISK());

        Dataset<Row> ms_ip = spark
                .read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://host.docker.internal:5432/diploma")
                .option("dbtable", "public.ms_ip")
                .option("user", "postgres")
                .option("password", "7844")
                .load()
//                .withColumnRenamed("probe", "_probe")
                .withColumnRenamed("imsi", "_imsi")
                .withColumnRenamed("msisdn", "_msisdn")
                .withColumnRenamed("start_time", "_start_time");

        Dataset<Row> exploded_ms_ip = ms_ip
                .withColumn("_ip", functions.explode(functions.split(trim(col("ms_ip_address")), ";")))
                .withColumn("_ip", trim(col("_ip")))
                .filter(col("_ip").notEqual(""));

        exploded_ms_ip.persist(StorageLevel.MEMORY_AND_DISK());

        exploded_ms_ip.show();


        Dataset<Row> joined_imsi_msisdn = withStartAndProbe
                .filter(col("imsi").isNotNull().and(not(col("imsi").like("999%"))))
                .join(
                        imsi_msisdn,
                        exploded_input.col("imsi").equalTo(imsi_msisdn.col("_imsi")),
                        "left_outer"
                )
                .withColumn("msisdn", coalesce(col("_msisdn"), col("msisdn")));


        Dataset<Row> joined_msip = exploded_input
                .filter(col("imsi").isNull().or(col("imsi").like("999%")))
                .join(
                        exploded_ms_ip,
//                        exploded_input.col("probe").equalTo(exploded_ms_ip.col("_probe"))
//                                .and(
                                exploded_input.col("ip").equalTo(exploded_ms_ip.col("_ip"))
//                                )
                                .and(exploded_input.col("start_time").$greater$eq(exploded_ms_ip.col("_start_time")))
                        ,
                        "left_outer"
                )
                .select(
                        exploded_input.col("*"),
                        exploded_ms_ip.col("_imsi"),
                        exploded_ms_ip.col("_msisdn"),
                        exploded_ms_ip.col("_start_time")
                )
                .withColumn("msisdn", coalesce(col("_msisdn"), col("msisdn")))
                .withColumn("imsi", coalesce(col("_imsi"), col("imsi")));


        StreamingQuery query1 = joined_msip
                .writeStream()
                .foreachBatch(
                        (VoidFunction2<Dataset<Row>, Long>) (dataset, batchId) -> {
                            if (!dataset.isEmpty()) {

                                Dataset<Row> filtered = dataset.sort(col("_start_time").desc()).limit(1);
                                filtered.show();

//                                filtered
//                                        .selectExpr(columnNames)
//                                        .write()
//                                        .mode("append")
//                                        .format("avro")
//                                        .option("avroSchema", "{\n" +
//                                                "  \"type\": \"record\",\n" +
//                                                "  \"name\": \"MyAvroRecord\",\n" +
//                                                "  \"fields\": [\n" +
//                                                "    {\"name\": \"measuring_probe_name\", \"type\": [\"null\", \"string\"]},\n" +
//                                                "    {\"name\": \"start_time\", \"type\": [\"null\", \"long\"]},\n" +
//                                                "    {\"name\": \"imsi\", \"type\": [\"null\", \"long\"]},\n" +
//                                                "    {\"name\": \"msisdn\", \"type\": [\"null\", \"long\"]},\n" +
//                                                "    {\"name\": \"ms_ip_address\", \"type\": [\"null\", \"string\"]}\n" +
//                                                "  ]\n" +
//                                                "}")
//                                        .partitionBy("event_date", "probe")
//                                        .option("path", "./results")
//                                        .option("checkpointLocation", "./path_to_checkpoint_location")
//                                        .option("compression", "uncompressed")
//                                        .save();

//                                System.out.println("\n\n\n!!!!!!!!!!!!!!!!\n" +
//                                        "Number of partitions: " + filtered.javaRDD().getNumPartitions() +
//                                        "\n!!!!!!!!!!!!!!\n\n\n");

                                filtered
                                        .selectExpr(columnNames)
                                        .write()
                                        .option("maxRecordsPerFile", 5)
//                                        .mode("overwrite")
                                        .mode("append")
                                        .format("parquet")
                                        .partitionBy("event_date", "probe")
//                                        .option("path", "./parquet_results")
                                        .option("path", "hdfs://namenode:8020/spark/results")
//                                        .option("checkpointLocation", "./path_to_checkpoint_location")
                                        .option("checkpointLocation", "hdfs://namenode:8020/spark/checkpoints")
                                        .save();

                            }
                        }
                )
                .start();

        StreamingQuery query2 = joined_imsi_msisdn
                .selectExpr(columnNames)
                .writeStream()
                .outputMode("append")
                .format("parquet")
                .partitionBy("event_date", "probe")
                .option("path", "hdfs://namenode:8020/spark/results")
                .option("checkpointLocation", "hdfs://namenode:8020/spark/checkpoints")

                .start();


//        StreamingQuery query = joined2
//                .writeStream()
//                .outputMode("append")
//                .format("avro")
//                .partitionBy("event_date", "probe")
//                .option("path", "./results")
//                .option("checkpointLocation", "./path_to_checkpoint_location")
//                .option("maxRecordsPerFile", 5)
//                .option("fileNamePrefix", "someprefix_")
////                  .option("spark.sql.avro.compression.codec", "uncompressed") // ??
//                .option("compression", "uncompressed")
//                .option("avroSchema", "{\n" +
//                        "  \"type\": \"record\",\n" +
//                        "  \"name\": \"MyAvroRecord\",\n" +
//                        "  \"fields\": [\n" +
//                        "    {\"name\": \"measuring_probe_name\", \"type\": [\"null\", \"string\"]},\n" +
//                        "    {\"name\": \"start_time\", \"type\": [\"null\", \"long\"]},\n" +
//                        "    {\"name\": \"imsi\", \"type\": [\"null\", \"long\"]},\n" +
//                        "    {\"name\": \"msisdn\", \"type\": [\"null\", \"long\"]},\n" +
//                        "    {\"name\": \"ms_ip_address\", \"type\": [\"null\", \"string\"]}\n" +
//                        "  ]\n" +
//                        "}")
//                .start();

        try {
            query1.awaitTermination();
            query2.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }



        // msip
//        val ref2DF = spark.table(s"$ref2Schema.$ref2Table").where(col("load_date") === processingTime)
//                .select(
//                        col("probe").as("_probe")
//                        , col("ms_ip_address").as("_ms_ip_address")
//                        , col("imsi").as("_imsi")
//                        , col("msisdn").as("_msisdn")
//                        , col("start_time").as("_start_time")
//                        , explode_outer(col("ms_ip_address")).as("_ip")
//                )
//
//        // GTPU
//        var srcDF = spark.read.parquet(prtPathStr)
//        val columns: Array[Column] = srcDF.columns.map(col)
//        srcDF = srcDF
//                .withColumn("probe", lit(probe).cast(StringType))
//                .withColumn("ms_ip_address",
//                        when(size(col("ms_ip_address")) < 1, colArray(lit(null).cast(StringType)))
//                                .otherwise(col("ms_ip_address").cast(ArrayType(StringType)))
//                )
//        //ref1DF = imsi_msisdn
//        val trg1DF = srcDF
//                .filter(col("imsi").isNotNull && !col("imsi").like("999%"))
//                .join(ref1DF, col("imsi") <=> col("_imsi"), "left_outer")
//          .withColumn("msisdn", coalesce(col("_msisdn"), col("msisdn")))
//                .select(columns: _*)
//
//        // ref2DF = msip
//        val trg2DF = srcDF
//                .filter(col("imsi").isNull || col("imsi").like("999%"))
//                .withColumn("id_p", monotonically_increasing_id())
//                .withColumn("ip", explode_outer(col("ms_ip_address")))
//                .join(ref2DF,
//                        (col("probe") <=> col("_probe"))
//                && col("ip") <=> col("_ip")
//                && col("_start_time") <= col("start_time")
//                , "left_outer")

//                .withColumn("row_number", row_number.over(Window.partitionBy("id_p").orderBy(col("_start_time").desc)))
//                .filter(col("row_number") === 1)

//                .withColumn("imsi", col("_imsi"))
//                .withColumn("msisdn", col("_msisdn"))
//                .select(columns: _*)


    }
}