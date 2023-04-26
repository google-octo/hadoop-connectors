package com.google.cloud.hadoop.perf.util;

import com.google.auto.value.AutoBuilder;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class WriteResult implements Closeable {

  private static final String PATH = "output/";

  private static final String FILE_PREFIX = "result";
  private static final String FILE_NAME_DELIMITER = "-";
  private final long libBufferSize;

  private List<WriteDataPointModel> orderedList = new LinkedList<>();

  WriteResult(long libBufferSize) throws IOException {
    this.libBufferSize = libBufferSize;
    Files.createDirectories(Paths.get(PATH));
  }

  private String getUniqueFileName() {
    String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
    String randomString = UUID.randomUUID().toString().substring(0, 5);
    return String.format(
        "%s%s%s%s%s.out",
        FILE_PREFIX, FILE_NAME_DELIMITER, randomString, FILE_NAME_DELIMITER, timeStamp);
  }

  public void addDatapoint(WriteDataPointModel dataPoint) {
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
      bw.write(WriteDataPointModel.getCsvHeader());
      for (WriteDataPointModel dp : orderedList) {
        bw.write(dp.toCommaSeparated());
      }
      bw.close();
    } catch (IOException e) {
      throw new IOException(
          String.format("Issues while writing data points to file:%s", fileName), e);
    }
    closeInternal();
  }

  public static WriteResult.Builder builder() {
    return new AutoBuilder_WriteResult_Builder();
  }

  @AutoBuilder(ofClass = WriteResult.class)
  public abstract static class Builder {

    public abstract WriteResult.Builder setLibBufferSize(long libBufferSize);

    public abstract WriteResult build() throws IOException;
  }
}
