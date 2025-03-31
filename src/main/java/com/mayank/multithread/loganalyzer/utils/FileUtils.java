package com.mayank.multithread.loganalyzer.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public final class FileUtils {
    private FileUtils() {
        // Prevent instantiation
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Returns the file name without its extension.
     *
     * @param fileName the name of the file
     * @return the file name without its extension
     */
    public static String getFileNameWithoutExtension(String fileName) {
        if (fileName == null || fileName.isEmpty()) {
            return fileName;
        }
        int lastDotIndex = fileName.lastIndexOf('.');
        if (lastDotIndex == -1) {
            return fileName;
        }
        return fileName.substring(0, lastDotIndex);
    }

    /**
     * Returns the current date and time formatted as "yyyyMMddHHmmss".
     * @return
     */
    public static String getCurrentFormattedDateTime() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        return now.format(formatter);
    }

}
