import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class StructuredStreamingApp {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        SparkSession spark = SparkSession
                .builder()
                .appName("spark")
                .getOrCreate();

        Dataset<Row> inputDF = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "test2")
                .load();

        Dataset<Row> splitted = inputDF
                .select(col("value").cast("string"))
                .select(split(col("value"), ",").alias("csv_values"));

        Dataset<Row> splitted2 = splitted
                .select(
                        col("csv_values").getItem(0).cast("string").as("measuring_probe_name"),
                        col("csv_values").getItem(1).cast("long").as("start_time"),
                        col("csv_values").getItem(2).cast("long").as("procedure_duration"),
                        col("csv_values").getItem(3).cast("string").as("subscriber_activity"),
                        col("csv_values").getItem(4).cast("long").as("procedure_type"),
                        col("csv_values").getItem(5).cast("long").as("imsi"),
                        col("csv_values").getItem(6).cast("long").as("msisdn"),
                        col("csv_values").getItem(7).cast("string").as("apn"),
                        col("csv_values").getItem(8).cast("string").as("uri"),
                        col("csv_values").getItem(9).cast("string").as("ms_ip_address")
//                        ,
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

        Dataset<Row> withStartAndProbe = splitted2
                .withColumn("start_date", functions.to_date(functions.from_unixtime(col("start_time").divide(1000))))
                .withColumn("probe", functions.substring(col("measuring_probe_name"), 1, 2));

        Dataset<Row> db_table1 = spark
                .read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://host.docker.internal:5432/diploma")
                .option("dbtable", "public.db_table1")
                .option("user", "postgres")
                .option("password", "7844")
                .load();

        Dataset<Row> db_table2 = spark
                .read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://host.docker.internal:5432/diploma")
                .option("dbtable", "public.db_table2")
                .option("user", "postgres")
                .option("password", "7844")
                .load();

        Dataset<Row> joined1 = withStartAndProbe
//                .where(col("imsi").isNull())
                .join(
                        db_table1,
                        withStartAndProbe.col("ms_ip_address").equalTo(db_table1.col("ms_ip_address")),
                        "left_outer"
                )
                .select(
                        withStartAndProbe.col("*"),
                        db_table1.col("imsi").alias("db_table1_imsi")
                )
                .drop(col("imsi"))
                .withColumnRenamed("db_table1_imsi", "imsi");

//        Dataset<Row> joined1 = withStartAndProbe
//                .join(
//                        db_table1,
//                        withStartAndProbe.col("ms_ip_address").equalTo(db_table1.col("ms_ip_address")),
//                        "left_outer"
//                )
//                .select(
//                        withStartAndProbe.col("*"),
//                        when(withStartAndProbe.col("imsi").isNull(), db_table1.col("imsi").alias("imsi"))
//                                .otherwise(withStartAndProbe.col("imsi")).alias("imsi")
//                );

        Dataset<Row> joined2 = joined1
                .join(
                        db_table2,
                        joined1.col("imsi").equalTo(db_table2.col("imsi")),
                        "left_outer"
                )
                .select(
                        joined1.col("*"),
                        db_table2.col("msisdn").alias("db_table2_msisdn")
                )
                .drop(col("msisdn"))
                .withColumnRenamed("db_table2_msisdn", "msisdn");

        StreamingQuery query = joined2
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