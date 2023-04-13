package com.google.cloud.hadoop.perf;

import static java.lang.Math.max;

import com.google.cloud.ReadChannel;
import com.google.cloud.hadoop.perf.util.BenchmarkConfigurations;
import com.google.cloud.hadoop.perf.util.BenchmarkConfigurations.BENCHMARK_TYPE_ENUM;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class JavaClientBenchmark {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final String fileName = "open_withItemInfo0";
  private final BenchmarkConfigurations benchmarkConfigurations;
  private final Storage client;

  public JavaClientBenchmark(BenchmarkConfigurations benchmarkConfigurations) {
    this.benchmarkConfigurations = benchmarkConfigurations;
    this.client = StorageOptions.grpc().setAttemptDirectPath(true).build().getService();
  }

  public void start() throws IOException {
    if (benchmarkConfigurations.getBenchmarkType() == BENCHMARK_TYPE_ENUM.RANDOM_READ) {
      logger.atInfo().log("Performing random read benchmark");
      randomRead();
    } else {
      seqRead();
    }
  }

  private void seqRead() throws IOException {

    long remaining = 300 * 1024 * 1024;
    BlobId blobId = BlobId.of("gcs-grpc-team-rdsingh", fileName);
    ReadChannel channel = client.reader(blobId);
    channel.seek(0);
    channel.limit(remaining);
    int countRead = 0;
    while (remaining > 0) {
      long startIndex = max(0, remaining - benchmarkConfigurations.getChunkSize());
      remaining = startIndex;
      ByteBuffer value = ByteBuffer.allocate(benchmarkConfigurations.getChunkSize());
      int bytesRead = channel.read(value);
      logger.atInfo().log("%d bytes read from file: %s ", bytesRead, fileName);
      countRead = countRead + 1;
    }

    logger.atInfo().log("read whole file and took %d iterations", countRead);
  }

  private void randomRead() throws IOException {

    logger.atInfo().log("file %s ", fileName);
    BlobId blobId = BlobId.of("gcs-grpc-team-rdsingh", fileName);

    long fileLength = 300 * 1024 * 1024;
    int fileSizeKB = (int) (fileLength / 1024);

    int chunkSize = benchmarkConfigurations.getChunkSize();
    int chunkSizeKB = chunkSize / 1024;

    int countRead = 0;

    for (int i = 0; i < benchmarkConfigurations.getReadCalls(); i++) {
      countRead += 1;
      Random r = new Random(i);
      long offset = (long) r.nextInt(fileSizeKB - chunkSizeKB) * 1024;
      ByteBuffer buff = ByteBuffer.allocate(chunkSize);

      long start = System.currentTimeMillis();
      ReadChannel channel = client.reader(blobId);
      channel.seek(offset);
      channel.limit(offset + chunkSize);
      int readBytes = channel.read(buff);
      long dur = System.currentTimeMillis() - start;
      channel.close();
      if (readBytes < chunkSize) {
        logger.atWarning().log("Got remaining bytes: %d", chunkSize - readBytes);
      }
      logger.atInfo().log("RANDOM-READ java-client grpc duration : %d", dur);
    }
    logger.atInfo().log("read whole file and took %d iterations", countRead);
  }
}
