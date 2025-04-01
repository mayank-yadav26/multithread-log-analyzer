# multithread-log-analyzer
## Overview

The **Multithread Log Analyzer** is a Spring Boot-based application designed to analyze multi-threaded logs efficiently. It allows users to upload log files, process them to extract key insights, and generate downloadable CSV reports. The application leverages **Apache Spark** for distributed log processing, ensuring optimal performance for large-scale log analysis.

## Features

- **File Upload & Processing:** Users can upload log files for analysis.

- **CSV File Generation & Download:** The processed log data is exported as a CSV file for further examination.

- **Thread Execution Time Calculation:** Utilizes Spark's to aggregate execution times per thread.

- **Most Frequent Log Message:** Utilizes Spark's to determine the most common log entry.

- **Max Thread Stop Time:** Identifies performance bottlenecks by calculating the maximum time a thread remains at a single log line.

## Tech Stack

- **Spring Boot** – Backend framework for building REST APIs

- **Apache Spark** – Distributed data processing engine for log analysis

- **REST APIs** – Facilitates seamless data interaction and integration

## API Endpoints

- **Upload Log File:** `POST /logs/upload`

- **Process Log File:** `POST /logs/process`

- **Download Processed CSV:** `GET /logs/download-csv`


