package com.google.cloud.hadoop.perf.util;

import com.google.auto.value.AutoBuilder;
import com.google.cloud.hadoop.perf.util.BenchmarkConfigurations.BENCHMARK_TYPE_ENUM;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class ReadResult implements Closeable {

  private static final String PATH = "output/";

  private static final String FILE_PREFIX = "result";
  private static final String FILE_NAME_DELIMITER = "-";
  private final long connectorChunkSize;
  private final long libBufferSize;
  private final String apiName;
  private final BENCHMARK_TYPE_ENUM benchmarkType;
  private List<DataPoint> orderedList = new LinkedList<>();

  ReadResult(
      long connectorReadChunkSize,
      long libBufferSize,
      String apiName,
      BENCHMARK_TYPE_ENUM benchmarkType)
      throws IOException {
    this.connectorChunkSize = connectorReadChunkSize;
    this.libBufferSize = libBufferSize;
    this.apiName = apiName;
    this.benchmarkType = benchmarkType;
    Files.createDirectories(Paths.get(PATH));
  }

  private String getUniqueFileName() {
    String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss").format(new java.util.Date());
    String randomString = UUID.randomUUID().toString().substring(0, 5);
    return String.format("%s%s%s%s.out", FILE_PREFIX, FILE_NAME_DELIMITER, randomString, FILE_NAME_DELIMITER, timeStamp);
  }

  public void addDatapoint(DataPoint dataPoint) {
    this.orderedList.add(dataPoint);
  }

  private void closeInternal() {
    orderedList = null;
  }

  @Override
  public void close() throws IOException {
    String fileName = getUniqueFileName();
    File file = new File(PATH + "/" + fileName);
    try {
      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);
      bw.write(DataPoint.getCsvHeader());
      for (DataPoint dp : orderedList) {
        bw.write(dp.toCommaSeparated());
      }
      bw.close();
    } catch (IOException e) {
      throw new IOException(
          String.format("Issues while writing data points to file:%s", fileName), e);
    }
    closeInternal();
  }

  public static Builder builder() {
    return new AutoBuilder_ReadResult_Builder();
  }

  @AutoBuilder(ofClass = ReadResult.class)
  public abstract static class Builder {

    public abstract ReadResult.Builder setConnectorReadChunkSize(long connectorReadChunkSize);

    public abstract ReadResult.Builder setLibBufferSize(long libBufferSize);

    public abstract ReadResult.Builder setApiName(String apiName);

    public abstract ReadResult.Builder setBenchmarkType(BENCHMARK_TYPE_ENUM benchmarkType);

    public abstract ReadResult build() throws IOException;
  }
}
