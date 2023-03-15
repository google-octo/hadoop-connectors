package com.google.cloud.hadoop.perf;

import static java.lang.Math.max;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

class ReadBenchmark {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private static final int CHUNK_SIZE = 2 * 1024 * 1024;
  private final String fileName = "open_withItemInfo0";
  private final GoogleHadoopFileSystem ghfs;
  private final ConnectorConfigurations connectorConfigurations = new ConnectorConfigurations();

  public ReadBenchmark(Map<String, String> configMap) throws IOException {
    this.ghfs = createGhfs(connectorConfigurations.getConfiguration(configMap));
  }

  protected void runBenchmark() throws IOException {
    FileStatus fs = ghfs.getFileStatus(new Path(fileName));

    logger.atInfo().log("file %s, length %d ", fileName, fs.getLen());
    FSDataInputStream inputStream = ghfs.open(new Path(fileName));
    long remaining = fs.getLen();
    int countRead = 0;
    while (remaining > 0) {
      long startIndex = max(0, remaining - CHUNK_SIZE);
      remaining = startIndex;
      byte[] value = new byte[CHUNK_SIZE];
      inputStream.seek(startIndex);
      int bytesRead = inputStream.read(value);
      logger.atInfo().log("%d bytes read from file: %s ", bytesRead, fileName);
      countRead = countRead + 1;
    }

    logger.atInfo().log("read whole file and took %d iterations", countRead);
  }

  private GoogleHadoopFileSystem createGhfs(Configuration config) throws IOException {
    GoogleHadoopFileSystem ghfs = new GoogleHadoopFileSystem();
    ghfs.initialize(new Path("gs://gcs-grpc-team-rdsingh").toUri(), config);
    return ghfs;
  }
}
