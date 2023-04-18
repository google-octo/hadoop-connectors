package com.google.cloud.hadoop.perf.util;

public class DataPoint {
  private static final char ELEMENT_DELIMITER = ',';
  private long elapsedTime = 0;
  private static final String ELAPSED_TIME = "ElapsedTime";
  private long readOffset = 0;
  private static final String READ_OFFSET = "ReadOffset";
  private long requestBytes = 0;
  private static final String BYTES_REQUESTED = "BytesRequested";
  private long bytesRead = 0;
  private static final String BYTES_READ = "BytesRead";
  private int threadNumber = 0;
  private static final String THREAD_NUMBER = "ThreadNumber";

  public void setElapsedTime(long elapsedTime) {
    this.elapsedTime = elapsedTime;
  }

  public void setReadOffset(long readOffset) {
    this.readOffset = readOffset;
  }

  public void setRequestBytes(long requestBytes) {
    this.requestBytes = requestBytes;
  }

  public void setThreadNumber(int threadNumber) {
    this.threadNumber = threadNumber;
  }

  public void setBytesRead(long bytesRead) {
    this.bytesRead = bytesRead;
  }

  public String toCommaSeparated() {
    String result = "";

    result += String.valueOf(threadNumber) + ELEMENT_DELIMITER;
    result += String.valueOf(requestBytes) + ELEMENT_DELIMITER;
    result += String.valueOf(readOffset) + ELEMENT_DELIMITER;
    result += String.valueOf(bytesRead) + ELEMENT_DELIMITER;
    result += String.valueOf(elapsedTime) + ELEMENT_DELIMITER;

    result += System.lineSeparator();
    return result;
  }

  public static String getCsvHeader() {
    String header = "";

    header += THREAD_NUMBER + ELEMENT_DELIMITER;
    header += BYTES_REQUESTED + ELEMENT_DELIMITER;
    header += READ_OFFSET + ELEMENT_DELIMITER;
    header += BYTES_READ + ELEMENT_DELIMITER;
    header += ELAPSED_TIME;

    header += System.lineSeparator();
    return header;
  }
}
