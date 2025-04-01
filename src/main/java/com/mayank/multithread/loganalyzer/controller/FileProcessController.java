package com.mayank.multithread.loganalyzer.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.mayank.multithread.loganalyzer.dto.LogProcessingResponse;
import com.mayank.multithread.loganalyzer.service.SparkFileProcessorService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api/files")
public class FileProcessController {
    private final SparkFileProcessorService sparkFileProcessorService;

    public FileProcessController(SparkFileProcessorService sparkFileProcessorService) {
        this.sparkFileProcessorService = sparkFileProcessorService;
    }

    @GetMapping("/process")
    public ResponseEntity<LogProcessingResponse> processLogFile(@RequestParam("fileName") String fileName) {
        log.debug("Inside processLogFile method: filename: {}", fileName);
        try {
            LogProcessingResponse response = sparkFileProcessorService.processLogFile(fileName);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(new LogProcessingResponse(fileName, 0, null, null, null, "Error processing file: " + e.getMessage(), null));
        }
    }
}
