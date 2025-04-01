package com.mayank.multithread.loganalyzer.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogProcessingResponse {
    private String fileName;
    private long lineCount;
    private String maxThreadExecution;
    private String mostFrequentMessage;
    private String maxThreadStopTime;
    private String message;
    private String downloadUrl;
}
