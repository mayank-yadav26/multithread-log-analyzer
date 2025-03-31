package com.mayank.multithread.loganalyzer.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

@Service("fileUploadService")
public class FileUploadService {

    @Value("${file.upload-dir}")
    private String uploadDir;

    public String saveFile(MultipartFile file) throws IOException {
        File directory = new File(uploadDir);
        if (!directory.exists()) {
            directory.mkdirs(); // Create directory if not exists
        }

        String filePath = Paths.get(uploadDir, file.getOriginalFilename()).toString();
        file.transferTo(new File(filePath));

        return filePath;
    }
}
