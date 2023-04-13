package com.google.cloud.hadoop.perf.util;

import java.util.HashMap;
import java.util.Map;

public class BenchmarkConfigurations {

  public static enum BENCHMARK_TYPE_ENUM {
    READ,
    RANDOM_READ,

    JAVA_STORAGE
  };

  private int chunkSize = 2 * 1024 * 1024;
  private int readCalls = 100;
  private BENCHMARK_TYPE_ENUM benchmarkType;

  private Map<String, String> configMap = new HashMap<>();

  public void setReadCalls(int readCalls) {
    this.readCalls = readCalls;
  }

  public int getReadCalls() {
    return this.readCalls;
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
