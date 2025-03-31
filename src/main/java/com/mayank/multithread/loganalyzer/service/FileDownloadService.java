package com.mayank.multithread.loganalyzer.service;

import java.io.File;
import java.io.IOException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service("fileDownloadService")
public class FileDownloadService {
    @Value("${file.upload-dir}")
    private String uploadDir;

    public String getFilePath(String fileName) throws IOException {
        File file = new File(uploadDir, fileName);
        if (!file.exists()) {
            throw new IOException("File not found: " + fileName);
        }
        return file.getAbsolutePath();
    }
}
