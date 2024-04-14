import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CompactionApp {

    //    private static final Logger log = LoggerFactory.getLogger(CompactionApp2.class);

    private static Config config;

    private static SparkSession spark;
    private static final long ONE_MB_IN_BYTES = 1024L * 1024L;

    public static void main(String[] args) throws IOException {

//        if (args.length < 1) {
//            throw new IllegalArgumentException("Path to config should be specified!");
//        }
//        config = ConfigFactory.parseFile(new File(args[0]));

        config = ConfigFactory.load("compaction.conf");

        spark = SparkSession.builder()
                .appName("CompactionApp")
                .getOrCreate();

        Configuration hdfsConf = new Configuration();
        hdfsConf.set("fs.defaultFS", config.getString("fs.defaultFS"));

        // Для отключения создания флага _SUCCESS
        spark.conf().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        ArrayList<Path> sourcePaths = Arrays.stream(config.getString("fs.defaultFS").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Path::new)
                .collect(Collectors.toCollection(ArrayList::new));

        String fileFormat = config.getString("source.format");
        long desiredFileSize = config.getLong("target.blockSizeMB") * ONE_MB_IN_BYTES;

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            for (Path path : sourcePaths) {
                try {
                    compactSourceDirectory(path, fileFormat, desiredFileSize, hdfsConf);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, 0, config.getLong("compaction.intervalSEC"), TimeUnit.SECONDS);

    }

    public static void compactSourceDirectory(Path path, String fileFormat, long desiredFileSize, Configuration hdfsConf) throws IOException {

        FileSystem fs = FileSystem.get(hdfsConf);

        ArrayList<FileStatus> dirsToBeCompacted = getDirsToBeCompacted(path, fs);
        System.out.println("dirsToBeCompacted: " + dirsToBeCompacted.toString());

        dirsToBeCompacted.parallelStream().forEach(directoryStatus -> {

            String currentSourceDirName = directoryStatus.getPath().toString();
            String fullCompactedPath = config.getString("target.path")
                    + "/" + directoryStatus.getPath().getParent().getName()
                    + "/" + directoryStatus.getPath().getName();

            long length = 0;
            try {
                ContentSummary contentSummary = fs.getContentSummary(directoryStatus.getPath());
                length = contentSummary.getLength();
            } catch (IOException e) {
                e.printStackTrace();
            }

            int repartition = (int) Math.ceil(Math.max(length * 1.2, desiredFileSize) / desiredFileSize);

            System.out.println("[" + directoryStatus.getPath().toString() + "]");
            System.out.println("currentSourceDirName: " + currentSourceDirName);
            System.out.println("fullCompactedPath: " + fullCompactedPath);
            System.out.println("length: " + length);
            System.out.println("repartition: " + repartition);

            System.setProperty("spark.job.description", "compact " + currentSourceDirName);

            int minParallelism = 50;
            if (repartition < minParallelism) {
                spark.read()
                        .parquet(currentSourceDirName)
                        .repartition(repartition)
                        .write()
                        .mode("append")
                        .format(fileFormat)
                        .save(fullCompactedPath);
            } else {
                spark.read()
                        .parquet(currentSourceDirName)
                        .coalesce(repartition)
                        .write()
                        .mode("append")
                        .format(fileFormat)
                        .save(fullCompactedPath);
            }

            try {
                deleteDirectory(directoryStatus.getPath(), hdfsConf);
            } catch (IOException e) {
                e.printStackTrace();
            }

        });

    }

    public static ArrayList<FileStatus> getDirsToBeCompacted(Path path, FileSystem fs) throws IOException {

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

        return dirsToBeCompacted;
    }

    public static void deleteDirectory(Path dirPath, Configuration hdfsConf) throws IOException {

        FileSystem fs = FileSystem.get(hdfsConf);

        if (fs.exists(dirPath)) {
            fs.delete(dirPath, true);
        }
        fs.close();

    }

}
