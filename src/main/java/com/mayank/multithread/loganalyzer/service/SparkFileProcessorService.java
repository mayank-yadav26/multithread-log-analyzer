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
import com.mayank.multithread.loganalyzer.dto.LogProcessingResponse;
import com.mayank.multithread.loganalyzer.utils.FileUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
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

    public LogProcessingResponse processLogFile(String fileName) {
        log.info("Processing log file: " + fileName);
        LogProcessingResponse response = new LogProcessingResponse();
        try {
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

            Dataset<Row> parsedLogsWithTime = calculateThreadTimeDiffForEachThread(parsedLogs, sparkSession, filePath);

            // Process the log file using Apache Spark
            long lineCount = logData.count();
            String maxThreadExecution = maxTimeTakingThread(parsedLogsWithTime);
            String mostFrequentMessage = maxNumberOfTimesSameLogMessage(parsedLogsWithTime);
            String maxThreadStopTime = maxTimeThreadStopped(parsedLogsWithTime);

            // Generate and save the CSV file
            String csvFileFolderName = FileUtils.getFileNameWithoutExtension(fileName);
            String baseUrl = "http://localhost:8089";
            String downloadUrl = baseUrl + "/api/files/download-csv?fileName=" + csvFileFolderName;

            // Create JSON response object
            response = new LogProcessingResponse(
                    fileName,
                    lineCount,
                    maxThreadExecution,
                    mostFrequentMessage,
                    maxThreadStopTime,
                    "Log file processed successfully.",
                    downloadUrl);

        } catch (Exception e) {
            log.error("Error processing log file: " + e);
            response.setMessage("Error processing log file: " + e.getMessage());
            response.setFileName(fileName);
        }
        log.info("Log file processed successfully: " + fileName);
        return response;
    }

    private Dataset<LogEntry> parseLogEntries(Dataset<String> logData) {
        log.info("Inside parseLogEntries method");
        // Use a static inner class for the FlatMapFunction to ensure it is serializable
        Dataset<LogEntry> parsedLogs = logData.flatMap(new LogEntryFlatMapFunction(), Encoders.bean(LogEntry.class));
        return parsedLogs;
    }

    // Static inner class for FlatMapFunction to ensure it is serializable
    private static class LogEntryFlatMapFunction implements FlatMapFunction<String, LogEntry> {
        private static final long serialVersionUID = 1L;
        private LogEntry previousEntry = null;
        private long lineNumber = 0;

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
                    log.error("Error parsing line: " + line);
                }
            } else if (previousEntry != null) {
                previousEntry.setMessage(previousEntry.getMessage() + "\n" + line.replaceAll("\"", ""));
            }
            return entries.iterator();
        }
    }

    /**
     * Find the thread with the maximum time difference
     * 
     * @param parsedLogsWithTime
     * @return
     */
    private String maxTimeTakingThread(Dataset<Row> parsedLogsWithTime) {
        log.info("Inside maxTimeTakingThread method");
        // Group by thread and sum the total time difference for each thread
        Dataset<Row> totalTimeDiffByThread = parsedLogsWithTime
                .groupBy("thread")
                .agg(functions.sum("time_diff_ms").alias("total_time_diff_ms"));

        // Get the thread with the maximum total time difference
        Row maxRow = totalTimeDiffByThread
                .orderBy(functions.desc("total_time_diff_ms")) // Sort in descending order
                .first(); // Take the first row (thread with max total time)

        // Extract thread name and total time difference
        String thread = maxRow.getAs("thread");
        double maxTimeDiffValue = maxRow.getAs("total_time_diff_ms");

        // Print and return the result
        String result = "Thread with maximum total time difference: " + thread +
                " with total time taken: " + maxTimeDiffValue + " ms";
        return result;
    }

    /**
     * Find the log message that is printed maximum times
     * 
     * @param parsedLogsWithTime
     * @return
     */
    private String maxNumberOfTimesSameLogMessage(Dataset<Row> parsedLogsWithTime) {
        log.info("Inside maxNumberOfTimesSameLogMessage method");
        // Find the maximum number of times the same log message is printed
        Dataset<Row> maxLogMessageCount = parsedLogsWithTime.groupBy("message")
                .agg(functions.count("message").alias("count"))
                .orderBy(functions.desc("count"));

        // Get the log message with the maximum count
        Row maxRow = maxLogMessageCount.first();
        String message = maxRow.getAs("message");
        long count = maxRow.getAs("count");
        log.debug("Log message printed maximum times: " + message + " with count: " + count);
        return "Log message printed maximum times: " + message + " with count: " + count;
    }

    // max time a thread stopped at a line: line number & time:
    private String maxTimeThreadStopped(Dataset<Row> parsedLogsWithTime) {
        log.info("Inside maxTimeThreadStopped method");
        // Find the row with the maximum time difference
        Dataset<Row> maxTimeDiffRow = parsedLogsWithTime
                .orderBy(functions.col("time_diff_ms").desc()) // Sort descending to get the max first
                .limit(1); // Keep only the top row

        // Get the maximum time difference row
        Row maxRow = maxTimeDiffRow.first();

        // Extract line number and time difference
        Object lineNumberObj = maxRow.getAs("lineNumber"); // Fetch as Object first
        String lineNumber = (lineNumberObj != null) ? lineNumberObj.toString() : "N/A"; // Convert to String safely
        double maxTimeDiffValue = maxRow.getAs("time_diff_ms");

        // Print and return the result
        String result = "Maximum time a thread stopped at a line: " + lineNumber +
                " with time difference: " + maxTimeDiffValue + " ms";
        return result;
    }

    /**
     * Calculate the time difference between each log entry and the next one for
     * each
     * 
     * @param parsedLogs
     * @param spark
     * @param filePath
     * @return
     */
    private Dataset<Row> calculateThreadTimeDiffForEachThread(Dataset<LogEntry> parsedLogs, SparkSession spark,
            String filePath) {
        log.info("Inside calculateThreadTimeDiffForEachThread method");
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
        FileUtils.saveFileToCSV(result, FileUtils.getFileNameWithoutExtension(filePath));
        return parsedLogsWithTime;
    }

}
