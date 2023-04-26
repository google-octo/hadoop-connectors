package com.google.cloud.hadoop.perf;

import static java.lang.Math.max;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.perf.util.BenchmarkConfigurations;
import com.google.cloud.hadoop.perf.util.BenchmarkConfigurations.BENCHMARK_TYPE_ENUM;
import com.google.cloud.hadoop.perf.util.ReadDataPointModel;
import com.google.cloud.hadoop.perf.util.ReadResult;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

class ReadBenchmark {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final String fileName = "open_withItemInfo0";
  private final GoogleHadoopFileSystem ghfs;
  private final ConnectorConfigurations connectorConfigurations = new ConnectorConfigurations();
  private final BenchmarkConfigurations benchmarkConfigurations;
  private final ReadResult readResult;
  private final Configuration configuration;

  public ReadBenchmark(BenchmarkConfigurations benchmarkConfigurations, ReadResult readResult)
      throws IOException {
    this.benchmarkConfigurations = benchmarkConfigurations;
    this.configuration =
        connectorConfigurations.getConfigurationFromMap(benchmarkConfigurations.getConfigMap());
    this.ghfs = createGhfs(configuration);
    this.readResult = readResult;
  }

  public void start() throws IOException {
    if (benchmarkConfigurations.getBenchmarkType() == BENCHMARK_TYPE_ENUM.RANDOM_READ) {
      logger.atInfo().log("Performing random read benchmark");
      randomRead();
    } else {
      seqRead();
    }
  }

  private boolean isGRPCTransport() {
    return (configuration.get("fs.gs.grpc.enable") == null
            || configuration.get("fs.gs.grpc.enable") == "false")
        ? false
        : true;
  }

  private String getApiName() {
    String apiName = "connector-read";
    String clientType = configuration.get("fs.gs.client.type");
    if (clientType != null && clientType.equalsIgnoreCase("STORAGE_CLIENT")) {
      apiName += "_java-storage";
    }

    if (isGRPCTransport()) {
      apiName += "_gRPC";
    } else {
      apiName += "_json";
    }
    return apiName;
  }

  private void seqRead() throws IOException {
    FileStatus fs = ghfs.getFileStatus(new Path(fileName));

    logger.atInfo().log("file %s, length %d ", fileName, fs.getLen());
    FSDataInputStream inputStream = ghfs.open(new Path(fileName));
    long remaining = fs.getLen();
    int countRead = 0;
    while (remaining > 0) {
      long startIndex = max(0, remaining - benchmarkConfigurations.getChunkSize());
      remaining = startIndex;
      byte[] value = new byte[benchmarkConfigurations.getChunkSize()];
      inputStream.seek(startIndex);
      int bytesRead = inputStream.read(value);
      logger.atInfo().log("%d bytes read from file: %s ", bytesRead, fileName);
      countRead = countRead + 1;
    }

    logger.atInfo().log("read whole file and took %d iterations", countRead);
  }

  private void randomRead() throws IOException {
    FileStatus fs = ghfs.getFileStatus(new Path(fileName));

    logger.atInfo().log("file %s, length %d ", fileName, fs.getLen());
    FSDataInputStream inputStream = ghfs.open(new Path(fileName));

    long fileLength = fs.getLen();
    int fileSizeKB = (int) (fileLength / 1024);

    int chunkSize = benchmarkConfigurations.getChunkSize();
    int chunkSizeKB = chunkSize / 1024;

    int countRead = 0;

    for (int i = 0; i < benchmarkConfigurations.getCallCount(); i++) {
      ReadDataPointModel dp = new ReadDataPointModel();
      dp.setApiName(getApiName());
      dp.setThreadNumber(1);
      dp.setBenchmarkType(benchmarkConfigurations.getBenchmarkType().toString());

      countRead += 1;
      Random r = new Random(i);
      long offset = (long) r.nextInt(fileSizeKB - chunkSizeKB) * 1024;
      dp.setReadOffset(offset);
      byte[] buff = new byte[chunkSize];
      long start = System.currentTimeMillis();
      inputStream.seek(offset);
      int readBytes = inputStream.read(buff);
      long dur = System.currentTimeMillis() - start;

      dp.setRequestBytes(buff.length);
      dp.setBytesRead(readBytes);
      dp.setElapsedTime(dur);
      readResult.addDatapoint(dp);

      if (readBytes < chunkSize) {
        logger.atWarning().log("Got remaining bytes: %d", chunkSize - readBytes);
      }
      logger.atInfo().log("RANDOM-READ duration : %d", dur);
    }
    logger.atInfo().log("read whole file and took %d iterations", countRead);
  }

  private GoogleHadoopFileSystem createGhfs(Configuration config) throws IOException {
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    logger.atInfo().log("Client library used %s", config.get("fs.gs.client.type"));
    ghfs.initialize(new Path("gs://gcs-grpc-team-rdsingh").toUri(), config);
    return ghfs;
  }
}
