package com.google.cloud.hadoop.perf.util;

public class WriteDataPointModel {
  private static final char ELEMENT_DELIMITER = ',';
  private long elapsedTime = 0;
  private static final String ELAPSED_TIME = "ElapsedTime";

  public void setElapsedTime(long elapsedTime) {
    this.elapsedTime = elapsedTime;
  }

  private long bytesWritten;
  private static final String BYTES_WRITTEN = "BytesWritten";

  public void setBytesWritten(long bytesWritten) {
    this.bytesWritten = bytesWritten;
  }

  private int threadNumber = 1;
  private static final String THREAD_NUMBER = "ThreadNumber";

  public void setThreadNumber(int threadNumber) {
    this.threadNumber = threadNumber;
  }

  private String apiName = "";
  private static final String API_NAME = "ApiName";

  public void setApiName(String apiName) {
    this.apiName = apiName;
  }

  private String benchmarkType = "";
  private static final String BENCHMARK_TYPE = "BenchmarkType";

  public void setBenchmarkType(String benchmarkType) {
    this.benchmarkType = benchmarkType;
  }

  public String toCommaSeparated() {
    String result = "";

    result += apiName + ELEMENT_DELIMITER;
    result += benchmarkType + ELEMENT_DELIMITER;
    result += String.valueOf(threadNumber) + ELEMENT_DELIMITER;
    result += String.valueOf(bytesWritten) + ELEMENT_DELIMITER;
    result += String.valueOf(elapsedTime);

    result += System.lineSeparator();
    return result;
  }

  public static String getCsvHeader() {
    String header = "";

    header += API_NAME + ELEMENT_DELIMITER;
    header += BENCHMARK_TYPE + ELEMENT_DELIMITER;
    header += THREAD_NUMBER + ELEMENT_DELIMITER;
    header += BYTES_WRITTEN + ELEMENT_DELIMITER;
    header += ELAPSED_TIME;

    header += System.lineSeparator();
    return header;
  }
}
