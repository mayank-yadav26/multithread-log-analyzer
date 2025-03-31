package com.mayank.multithread.loganalyzer.controller;

import java.io.IOException;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.mayank.multithread.loganalyzer.service.FileUploadService;

@RestController
@RequestMapping("/api/files")
public class FileUploadController {

    private final FileUploadService fileUplaodService;

    public FileUploadController(FileUploadService fileUplaodService) {
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
}
