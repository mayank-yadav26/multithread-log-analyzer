package com.mayank.multithread.loganalyzer.controller;

import java.io.IOException;

import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.mayank.multithread.loganalyzer.service.FileDownloadService;

@RestController
@RequestMapping("/api/files")
public class FileDownloadController {
    private final FileDownloadService fileDownloadService;

    FileDownloadController(FileDownloadService fileDownloadService) {
        this.fileDownloadService = fileDownloadService;
    }

    @GetMapping("/download")
    public ResponseEntity<Resource> downloadFile(@RequestParam("fileName") String fileName) {
        try {
            String filePath = fileDownloadService.getFilePath(fileName);
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
}
