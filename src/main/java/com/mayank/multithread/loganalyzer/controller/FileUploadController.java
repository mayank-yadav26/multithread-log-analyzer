package com.mayank.multithread.loganalyzer.controller;

import java.io.IOException;

import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.mayank.multithread.loganalyzer.service.FileUploadService;
import com.mayank.multithread.loganalyzer.service.SparkFileProcessorService;

@RestController
@RequestMapping("/api/files")
public class FileUploadController {

    private final FileUploadService fileUplaodService;
    private final SparkFileProcessorService sparkFileProcessorService;

    public FileUploadController(FileUploadService fileUplaodService, SparkFileProcessorService sparkFileProcessorService) {
        this.sparkFileProcessorService = sparkFileProcessorService;
        this.fileUplaodService = fileUplaodService;
    }

    @PostMapping("/upload")
    public ResponseEntity<String> uploadFile(@RequestParam("file") MultipartFile file) {
        try {
            String filePath = fileUplaodService.saveFile(file);
            return ResponseEntity.ok("File uploaded successfully: " + filePath);
        } catch (IOException e) {
            return ResponseEntity.internalServerError().body("Failed to upload file: " + e.getMessage());
        }
    }

    @GetMapping("/download")
    public ResponseEntity<Resource> downloadFile(@RequestParam("fileName") String fileName) {
        try {
            String filePath = fileUplaodService.getFilePath(fileName);
            Resource resource = new UrlResource("file:" + filePath);
            if (!resource.exists() || !resource.isReadable()) {
                throw new IOException("File not found or not readable: " + fileName);
            }
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"")
                    .body(resource);
        } catch (IOException e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/process")
    public ResponseEntity<String> processLogFile(@RequestParam("fileName") String fileName) {
        try {
            String result = sparkFileProcessorService.processLogFile(fileName);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Failed to process file: " + e.getMessage());
        }
    }
}
