package com.google.cloud.hadoop.perf.util;

import java.util.HashMap;
import java.util.Map;

public class BenchmarkConfigurations {

  public static enum BENCHMARK_TYPE_ENUM {
    READ,
    RANDOM_READ,
    WRITE,
    JAVA_STORAGE_READ,
    JAVA_STORAGE_WRITE
  };

  private int chunkSize = 2 * 1024 * 1024;
  private int callCount = 100;
  private long writeFileSize = 1 * 1024 * 1024;
  private BENCHMARK_TYPE_ENUM benchmarkType;

  private Map<String, String> configMap = new HashMap<>();

  public void setCallCount(int callCount) {
    this.callCount = callCount;
  }

  public void setWriteFileSize(long writeFileSize) {
    this.writeFileSize = writeFileSize;
  }

  public long getWriteFileSize() {
    return writeFileSize;
  }

  public int getCallCount() {
    return this.callCount;
  }

  public int getChunkSize() {
    return chunkSize;
  }

  public void setChunkSize(int chunkSize) {
    this.chunkSize = chunkSize;
  }

  public Map<String, String> getConfigMap() {
    return configMap;
  }

  public void setConfigMap(Map<String, String> configMap) {
    this.configMap = configMap;
  }

  public BENCHMARK_TYPE_ENUM getBenchmarkType() {
    return benchmarkType;
  }

  public void setBenchmarkType(BENCHMARK_TYPE_ENUM benchmarkType) {
    this.benchmarkType = benchmarkType;
  }
}
