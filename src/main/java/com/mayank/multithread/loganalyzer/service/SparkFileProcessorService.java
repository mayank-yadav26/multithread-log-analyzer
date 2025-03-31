package com.mayank.multithread.loganalyzer.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.mayank.multithread.loganalyzer.dto.LogEntry;
import com.mayank.multithread.loganalyzer.utils.FileUtils;

@Service("sparkFileProcessorService")
public class SparkFileProcessorService {
    @Value("${file.upload-dir}")
    private String uploadDir;

    private final SparkSession sparkSession;

    public SparkFileProcessorService() {
        SparkConf conf = new SparkConf()
                .setAppName("LogAnalyzer")
                .setMaster("local[*]")
                .set("spark.ui.enabled", "false"); // Disable UI to avoid servlet issues

        this.sparkSession = SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }

    public String processLogFile(String fileName) {
        String filePath = uploadDir + "/" + fileName;

        // Read the log file into a DataFrame
        Dataset<String> logData = sparkSession.read().textFile(filePath);

        // Create a Dataset with the last line, to handle last line process using spark
        Dataset<String> lastLine = sparkSession.createDataset(java.util.Collections.singletonList("[ last line ]"),
                Encoders.STRING());

        // Append the last line to the original Dataset
        logData = logData.union(lastLine);

        // Coalesce to a single partition to ensure a single output file
        logData = logData.coalesce(1);

        // Parse the log entries
        Dataset<LogEntry> parsedLogs = parseLogEntries(logData);

        calculateThreadTimeDiffForEachThread(parsedLogs, sparkSession, filePath);

        // Perform some processing on the log data
        // For example, count the number of lines in the log file
        long lineCount = logData.count();

        // Return the result as a string
        return "Number of lines in " + fileName + ": " + lineCount;
    }

    private static Dataset<LogEntry> parseLogEntries(Dataset<String> logData) {
        // Parse log entries
        Dataset<LogEntry> parsedLogs = logData.flatMap(new FlatMapFunction<String, LogEntry>() {
            private static final long serialVersionUID = 1L;
            private LogEntry previousEntry = null;
            long lineNumber = 0;

            @Override
            public Iterator<LogEntry> call(String line) {
                lineNumber++;
                List<LogEntry> entries = new ArrayList<>();
                if (line.startsWith("[")) {
                    if (previousEntry != null) {
                        entries.add(previousEntry);
                    }

                    String[] parts = line.split(" ", 7);
                    try {
                        LogEntry entry = new LogEntry();
                        entry.setLineNumber(lineNumber);
                        entry.setTimestamp(parts[0].substring(1) + " " + parts[1].substring(0, parts[1].length()));
                        entry.setZone(parts[2].substring(0, parts[2].length() - 1));
                        entry.setThread(parts[3].substring(1, parts[3].length() - 1));
                        entry.setIp(parts[4].substring(1, parts[4].length() - 1));
                        entry.setAccountId(parts[5].substring(1, parts[5].length() - 1));
                        entry.setLogLevel((parts[6].substring(0, 5)).trim());
                        entry.setClassName(parts[6].split(":", 2)[0].substring(6).trim());
                        entry.setClassLineNumber(parts[6].split(":", 2)[1].split(" - ", 2)[0].trim());
                        entry.setMessage(parts[6].split(" - ", 2)[1].replaceAll("\"", ""));
                        previousEntry = entry;
                    } catch (Exception e) {
                        // Handle any parsing exceptions if needed
                        System.err.println("Error parsing line: " + line);
                    }
                } else if (previousEntry != null) {
                    previousEntry.setMessage(previousEntry.getMessage() + "\n" + line.replaceAll("\"", ""));
                }
                return entries.iterator();
            }
        }, Encoders.bean(LogEntry.class));
        return parsedLogs;
    }

    private static void calculateThreadTimeDiffForEachThread(Dataset<LogEntry> parsedLogs, SparkSession spark,
            String filePath) {
        // Convert timestamp to a proper format
        Dataset<Row> parsedLogsWithTime = parsedLogs.withColumn("timestamp_initial", parsedLogs.col("timestamp"))
                .withColumn("timestamp", functions.to_timestamp(parsedLogs.col("timestamp"), "MM-dd-yy HH:mm:ss:SSS"));

        // Define a window specification to partition by thread and order by timestamp
        WindowSpec windowSpec = Window.partitionBy("thread").orderBy("timestamp");

        // Use the lead function to get the next timestamp within each thread group
        parsedLogsWithTime = parsedLogsWithTime.withColumn("next_timestamp",
                functions.lead("timestamp", 1).over(windowSpec));

        parsedLogsWithTime.show();
        // Calculate the time difference between the current and next timestamp in
        // milliseconds
        parsedLogsWithTime = parsedLogsWithTime.withColumn("time_diff_ms",
                functions.expr("unix_timestamp(next_timestamp) * 1000 + date_format(next_timestamp, 'SSS') - "
                        + "(unix_timestamp(timestamp) * 1000 + date_format(timestamp, 'SSS'))"));

        // Create a temporary view
        parsedLogsWithTime.createOrReplaceTempView("logs");

        // Use SQL to select columns in the desired order
        Dataset<Row> result = spark
                .sql("""
                        SELECT lineNumber,time_diff_ms,timestamp_initial,thread, accountId, logLevel, className,classLineNumber, message
                        FROM logs
                        """);

        // Save the result to a file with headers and quote all fields
        result.write().option("header", "true").option("quoteAll", "true").format("csv")
                .save(FileUtils.getFileNameWithoutExtension(filePath) + FileUtils.getCurrentFormattedDateTime());

    }

}
