package com.mayank.multithread.loganalyzer.service;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service("sparkFileProcessorService")
public class SparkFileProcessorService {
    @Value("${file.upload-dir}")
    private String uploadDir;

    private final SparkSession sparkSession;

    public SparkFileProcessorService() {
        SparkConf conf = new SparkConf()
                .setAppName("LogAnalyzer")
                .setMaster("local[*]")
                .set("spark.ui.enabled", "false");  // Disable UI to avoid servlet issues

        this.sparkSession = SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }

    public String processLogFile(String fileName) {
        String filePath = uploadDir + "/" + fileName;

        // Read the log file into a DataFrame
        Dataset<Row> logData = sparkSession.read().text(filePath);

        // Perform some processing on the log data
        // For example, count the number of lines in the log file
        long lineCount = logData.count();

        // Return the result as a string
        return "Number of lines in " + fileName + ": " + lineCount;
    }
}
