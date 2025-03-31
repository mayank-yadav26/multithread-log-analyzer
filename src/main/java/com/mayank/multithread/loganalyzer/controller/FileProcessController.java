package com.mayank.multithread.loganalyzer.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.mayank.multithread.loganalyzer.service.SparkFileProcessorService;

@RestController
@RequestMapping("/api/files")
public class FileProcessController {
    private final SparkFileProcessorService sparkFileProcessorService;

    public FileProcessController(SparkFileProcessorService sparkFileProcessorService) {
        this.sparkFileProcessorService = sparkFileProcessorService;
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
