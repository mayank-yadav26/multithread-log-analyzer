package com.mayank.multithread.dto;

import lombok.Data;

@Data
public class LogEntry {
    private long lineNumber;
    private String timestamp;
    private String ip;
    private String zone;
    private String thread;
    private String accountId;
    private String logLevel;
    private String className;
    private String classLineNumber;
    private String message;
}
