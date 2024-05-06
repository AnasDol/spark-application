import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.storage.StorageLevel;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class EnrichmentApp {

    private static Config config;
    private static SparkSession spark;

    public static void main(String[] args) throws TimeoutException {


        if (args.length < 1) {
            config = ConfigFactory.load("spark.conf");
        } else {
            config = ConfigFactory.parseFile(new File(args[0]));
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.sql.streaming.metricsEnabled", "true");

        spark = SparkSession
                .builder()
                .appName(config.getString("application.name"))
                .config(sparkConf)
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        run();

    }

    public static void run() throws TimeoutException {

        Dataset<Row> srcWithPartitionColumns = addPartitionColumns(splitCSV(getKafkaSource()));
        String[] columnNames = srcWithPartitionColumns.columns(); // схема результирующей строки совпадает с srcWithPartitionColumns
        Dataset<Row> srcExploded = explodeByMsIpAddress(srcWithPartitionColumns, "ip");


        Dataset<Row> imsiMsisdn = getImsiMsisdn();

        ScheduledExecutorService imsiMsisdnScheduler = Executors.newScheduledThreadPool(1);
        imsiMsisdnScheduler.scheduleAtFixedRate(
                () -> {
                    imsiMsisdn.unpersist();
                    imsiMsisdn.persist(StorageLevel.MEMORY_AND_DISK());  // lazy!
                    // imsiMsisdn.count(); // to make it eager
                },
                config.getLong("imsi_msisdn.cache.initial_delay_min"),
                config.getLong("imsi_msisdn.cache.period_min"),
                TimeUnit.MINUTES
        );


        Dataset<Row> msIpExploded = explodeByMsIpAddress(getMsIp(), "_ip");

        ScheduledExecutorService msIpScheduler = Executors.newScheduledThreadPool(1);
        msIpScheduler.scheduleAtFixedRate(
                () -> {
                    msIpExploded.unpersist();
                    msIpExploded.persist(StorageLevel.MEMORY_AND_DISK()); // lazy!
                    // msIpExploded.count(); // to make it eager
                },
                config.getLong("ms_ip.cache.initial_delay_min"),
                config.getLong("ms_ip.cache.period_min"),
                TimeUnit.MINUTES
        );


        Dataset<Row> joinedImsiMsisdn = joinImsiMsisdn(srcWithPartitionColumns, imsiMsisdn);
        Dataset<Row> joinedMsIp = joinMsIp(srcExploded, msIpExploded);


        StreamingQuery msIpQuery = joinedMsIp
            .writeStream()
            .foreachBatch(
                    (VoidFunction2<Dataset<Row>, Long>) (dataset, batchId) -> {
                        if (!dataset.isEmpty()) {

                            WindowSpec windowSpec = Window.partitionBy("unique_cdr_id").orderBy(col("_start_time").desc());

                            // Получаем строки с наибольшим значением _start_time для каждого unique_cdr_id
                            Dataset<Row> maxRows = dataset.withColumn("rank", row_number().over(windowSpec))
                                    .filter(col("rank").equalTo(1))
                                    .drop("rank");

                            maxRows.selectExpr(columnNames).show();

//                            Dataset<Row> filtered = dataset.sort(col("_start_time").desc()).limit(1);
//                            filtered.selectExpr(columnNames).show();

//                            filtered
//                                    .selectExpr(columnNames)
//                                    .write()
//                                    .mode("append")
//                                    .format(config.getString("sink.hdfs.format"))
//                                    .partitionBy("event_date", "probe")
//                                    .option("path", config.getString("sink.hdfs.path"))
//                                    .option("checkpointLocation", config.getString("sink.hdfs.checkpointLocation"))
//                                    .save();

                        }
                    }
                )
                .queryName("MsIp Query")
                .start();

        // sink в консоль
        StreamingQuery imsiMsisdnQuery = joinedImsiMsisdn
                .selectExpr(columnNames)
                .writeStream()
                .outputMode("append")
                .format("console") // изменение формата вывода на консоль
                .partitionBy("event_date", "probe")
                .queryName("ImsiMsisdn Query")
                .start();

//        StreamingQuery imsiMsisdnQuery = joinedImsiMsisdn
//                .selectExpr(columnNames)
//                .writeStream()
//                .outputMode("append")
//                .format("parquet")
//                .partitionBy("event_date", "probe")
//                .option("path", config.getString("sink.hdfs.path"))
//                .option("checkpointLocation", config.getString("sink.hdfs.checkpointLocation"))
//                .start();

        try {
            msIpQuery.awaitTermination();
            imsiMsisdnQuery.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

        imsiMsisdnScheduler.shutdown();
        msIpScheduler.shutdown();

    }


    private static Dataset<Row> getKafkaSource() {
        return spark
                .readStream()
                .format(config.getString("source.format"))
                .option("kafka.bootstrap.servers", config.getString("source.kafka.bootstrap.servers"))
                .option("subscribe", config.getString("source.subscribe"))
                .option("failOnDataLoss", config.getString("source.failOnDataLoss"))
                .load();
    }

    private static Dataset<Row> splitCSV(Dataset<Row> src) {
        return src
                .select(col("value").cast("string"))
                .select(split(col("value"), ",").alias("csv_values"))
                .select(
                        col("csv_values").getItem(0).cast("timestamp").as("start_time"),
                        col("csv_values").getItem(1).cast("string").as("measuring_probe_name"),
                        col("csv_values").getItem(2).cast("long").as("imsi"),
                        col("csv_values").getItem(3).cast("long").as("msisdn"),
                        col("csv_values").getItem(4).cast("string").as("ms_ip_address"),
                        col("csv_values").getItem(5).cast("long").as("unique_cdr_id")
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
    }


    private static Dataset<Row> addPartitionColumns(Dataset<Row> src) {
        return src
                .withColumn("event_date", functions.date_format(functions.to_date(col("start_time")), "yyyy-MM-dd"))
                .withColumn("probe", functions.substring(col("measuring_probe_name"), 1, 2));
    }

    private static Dataset<Row> explodeByMsIpAddress(Dataset<Row> dataset, String newColumnName) {
        return dataset
                .withColumn(newColumnName, functions.explode(functions.split(trim(col("ms_ip_address")), ";")))
                .withColumn(newColumnName, trim(col(newColumnName)))
                .filter(col(newColumnName).notEqual(""));

    }

    private static Dataset<Row> getImsiMsisdn() {
        return spark
                .read()
                .format(config.getString("imsi_msisdn.format"))
                .option("url", config.getString("imsi_msisdn.url"))
                .option("dbtable", config.getString("imsi_msisdn.dbtable"))
                .option("user", config.getString("imsi_msisdn.user"))
                .option("password", config.getString("imsi_msisdn.password"))
                .load()
                .withColumnRenamed("imsi", "_imsi")
                .withColumnRenamed("msisdn", "_msisdn");
    }

    private static Dataset<Row> getMsIp() {
        return spark
                .read()
                .format(config.getString("ms_ip.format"))
                .option("url", config.getString("ms_ip.url"))
                .option("dbtable", config.getString("ms_ip.dbtable"))
                .option("user", config.getString("ms_ip.user"))
                .option("password", config.getString("ms_ip.password"))
                .load()
                .withColumnRenamed("probe", "_probe")
                .withColumnRenamed("imsi", "_imsi")
                .withColumnRenamed("msisdn", "_msisdn")
                .withColumnRenamed("start_time", "_start_time");
    }

    private static Dataset<Row> joinImsiMsisdn(Dataset<Row> srcWithPartitionColumns, Dataset<Row> imsiMsisdn) {
        return srcWithPartitionColumns
                .filter(col("imsi").isNotNull().and(not(col("imsi").like("999%"))))
                .join(
                        imsiMsisdn,
                        srcWithPartitionColumns.col("imsi").equalTo(imsiMsisdn.col("_imsi")),
                        "left_outer"
                )
                .withColumn("msisdn", coalesce(col("_msisdn"), col("msisdn")));
    }

    private static Dataset<Row> joinMsIp(Dataset<Row> srcExploded, Dataset<Row> msIpExploded) {
        return srcExploded
                .filter(col("imsi").isNull().or(col("imsi").like("999%")))
                .join(
                        msIpExploded,
                        srcExploded.col("probe").equalTo(msIpExploded.col("_probe"))
                                .and(srcExploded.col("ip").equalTo(msIpExploded.col("_ip")))
                                .and(srcExploded
                                        .col("start_time")
                                        .$greater$eq(msIpExploded.col("_start_time"))
                                )
                        ,
                        "left_outer"
                )
                .select(
                        srcExploded.col("*"),
                        msIpExploded.col("_imsi"),
                        msIpExploded.col("_msisdn"),
                        msIpExploded.col("_start_time")
                )
                .withColumn("msisdn", coalesce(col("_msisdn"), col("msisdn")))
                .withColumn("imsi", coalesce(col("_imsi"), col("imsi")));
    }

}