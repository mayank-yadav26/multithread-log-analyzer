package com.mayank.multithread.loganalyzer.service;

import java.io.File;
import java.io.IOException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service("fileDownloadService")
public class FileDownloadService {
    @Value("${file.upload-dir}")
    private String uploadDir;


    public String getGeneratedCsvFilePath(String fileName) {
        String directoryPath = uploadDir + File.separator + fileName;
        File dir = new File(directoryPath);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new RuntimeException("Directory not found: " + directoryPath);
        }

        for (File file : dir.listFiles()) {
            if (file.getName().endsWith(".csv")) {
                return file.getAbsolutePath();
            }
        }
        throw new RuntimeException("No CSV file found in directory: " + directoryPath);
    }
}
