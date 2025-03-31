package com.mayank.multithread.loganalyzer.controller;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
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

    @GetMapping("/download-csv")
    public ResponseEntity<Resource> downloadCSVFile(@RequestParam("fileName") String fileName) {
        try {
            String csvFilePath = fileDownloadService.getGeneratedCsvFilePath(fileName);
            // Convert to Path
            Path path = Paths.get(csvFilePath);
            Resource resource = new UrlResource(path.toUri());

            if (!resource.exists() || !resource.isReadable()) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
            }

            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + ".csv" + "\"")
                    .header(HttpHeaders.CONTENT_TYPE, "text/csv")
                    .body(resource);

        } catch (IOException e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
