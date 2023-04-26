package com.google.cloud.hadoop.perf;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.perf.util.BenchmarkConfigurations;
import com.google.cloud.hadoop.perf.util.WriteDataPointModel;
import com.google.cloud.hadoop.perf.util.WriteResult;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class WriteBenchmark {

  private static final String PATH = "/benchmarks/write_benchmark";
  private static final String DELIMITER = "/";

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private final BenchmarkConfigurations benchmarkConfigurations;
  private final ConnectorConfigurations connectorConfigurations = new ConnectorConfigurations();
  private final Configuration configuration;
  private final GoogleHadoopFileSystem ghfs;
  private final WriteResult writeResult;

  public WriteBenchmark(BenchmarkConfigurations benchmarkConfigurations, WriteResult writeResult)
      throws IOException {
    this.benchmarkConfigurations = benchmarkConfigurations;
    this.configuration =
        connectorConfigurations.getConfigurationFromMap(benchmarkConfigurations.getConfigMap());
    this.ghfs = createGhfs(configuration);
    this.writeResult = writeResult;
  }

  public void start() throws IOException {
    gcsWrite();
  }

  private boolean isGRPCTransport() {
    return (configuration.get("fs.gs.grpc.enable") == null
            || configuration.get("fs.gs.grpc.enable") == "false")
        ? false
        : true;
  }

  private String getApiName() {
    String apiName = "connector-write";
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

  private String getUniqueFilePath() {
    String timeStamp = new SimpleDateFormat("yyyy-MM-dd/HH-mm").format(new Date());
    String randomString = UUID.randomUUID().toString();
    return String.format("%s%s%s%s%s.out", PATH, DELIMITER, timeStamp, DELIMITER, randomString);
  }

  private void gcsWrite() throws IOException {

    for (int i = 0; i < benchmarkConfigurations.getCallCount(); i++) {
      WriteDataPointModel dp = new WriteDataPointModel();
      dp.setApiName(getApiName());
      dp.setThreadNumber(1);
      dp.setBenchmarkType(benchmarkConfigurations.getBenchmarkType().toString());
      String fileName = getUniqueFilePath();

      FSDataOutputStream outputStream = ghfs.create(new Path(fileName));
      logger.atInfo().log("creating file: %s", fileName);

      long fileSize = benchmarkConfigurations.getWriteFileSize();
      long writtenBytes = 0;
      long dur = 0;

      while (writtenBytes < fileSize) {
        long chunkSize = 1024;
        byte[] randBytes = new byte[(int) chunkSize];
        ThreadLocalRandom.current().nextBytes(randBytes);

        long start = System.currentTimeMillis();
        outputStream.write(randBytes);
        dur += System.currentTimeMillis() - start;

        writtenBytes += chunkSize;
      }
      long start = System.currentTimeMillis();
      outputStream.close();
      dur += System.currentTimeMillis() - start;
      dp.setBytesWritten(fileSize);
      dp.setElapsedTime(dur);
      writeResult.addDatapoint(dp);
      logger.atInfo().log("WRITE duration : %d", dur);
      logger.atInfo().log("file number %d is written , took %d ms", i + 1, dur);
    }
  }

  private GoogleHadoopFileSystem createGhfs(Configuration config) throws IOException {
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    logger.atInfo().log("Client library used %s", config.get("fs.gs.client.type"));
    ghfs.initialize(new Path("gs://gcs-grpc-team-rdsingh").toUri(), config);
    return ghfs;
  }
}
