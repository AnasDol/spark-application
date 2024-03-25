import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;

public class CompactionApp {

    //    private static final Logger log = LoggerFactory.getLogger(CompactionApp2.class);
    private static final long ONE_MB_IN_BYTES = 1024L * 1024L;
    private static final int HDFS_BLOCK_SIZE_MB = 128;
    private static final long HDFS_BLOCK_SIZE_BYTES = HDFS_BLOCK_SIZE_MB * ONE_MB_IN_BYTES;

    private static SparkSession spark;

    public static void main(String[] args) throws IOException {

        spark = SparkSession.builder()
                .appName("CompactionApp")
                .getOrCreate();

        // Для отключения создания флага _SUCCESS
        spark.conf().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        ArrayList<Path> paths = new ArrayList<>();
        paths.add(new Path("hdfs://namenode:8020/spark/results"));

        String fileFormat = "parquet";
        long fileSize = HDFS_BLOCK_SIZE_BYTES;

        for (Path path : paths) {
            compactDirectory(path, fileFormat, fileSize, new Configuration());
        }

    }

    public static void compactDirectory(Path path, String fileFormat, long desiredFileSize, Configuration hdfsConf) throws IOException {

        FileSystem fs = FileSystem.get(hdfsConf);

        ArrayList<FileStatus> dirsToBeCompacted = new ArrayList<>();

        FileStatus[] allFilesInPath = fs.listStatus(path);
        for (FileStatus eventDateDir : allFilesInPath) {
            if (eventDateDir.isDirectory() && eventDateDir.getPath().getName().startsWith("event_date=")) {
                FileStatus[] allFilesInEventDateDir = fs.listStatus(eventDateDir.getPath());
                for (FileStatus probeDir : allFilesInEventDateDir) {
                    if (probeDir.isDirectory() && probeDir.getPath().getName().startsWith("probe=")) {
                        dirsToBeCompacted.add(probeDir);
                    }
                }
            }
        }

        dirsToBeCompacted.parallelStream().map(directoryStatus -> {

            String readPath = directoryStatus.getPath().toString();
            String compactedPath = "hdfs://namenode:8020/spark/compacted/"
                    + directoryStatus.getPath().getParent().getName()
                    + "/" + directoryStatus.getPath().getName();

            long length = 0;
            try {
                ContentSummary contentSummary = fs.getContentSummary(directoryStatus.getPath());
                length = contentSummary.getLength();
            } catch (IOException e) {
                e.printStackTrace();
            }

            int repartition = (int) Math.ceil(Math.max(length * 1.2, desiredFileSize) / desiredFileSize);

            System.setProperty("spark.job.description", "compact " + readPath);

            int minParallelism = 50;
            if (repartition < minParallelism) {
                spark.read()
                        .parquet(readPath)
                        .repartition(repartition)
                        .write()
                        .mode("append")
                        .format(fileFormat)
                        .save(compactedPath);
            } else {
                spark.read()
                        .parquet(readPath)
                        .coalesce(repartition)
                        .write()
                        .mode("append")
                        .format(fileFormat)
                        .save(compactedPath);
            }

            try {
                deleteDirectory(directoryStatus.getPath(), hdfsConf);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        });

    }

    public static void deleteDirectory(Path dirPath, Configuration hdfsConf) throws IOException {

        FileSystem fs = FileSystem.get(hdfsConf);

        if (fs.exists(dirPath)) {
            fs.delete(dirPath, true);
        }
        fs.close();

    }

}
