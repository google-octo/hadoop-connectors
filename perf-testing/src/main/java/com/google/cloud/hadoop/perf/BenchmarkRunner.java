package com.google.cloud.hadoop.perf;

import com.google.cloud.hadoop.perf.util.Args;
import com.google.cloud.hadoop.perf.util.BenchmarkConfigurations;
import com.google.cloud.hadoop.perf.util.WriteResult;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;

public class BenchmarkRunner {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static void main(String[] args) throws IOException {
    try {
      String classpath = System.getenv("CLASS_PATH");
      String grpcVariable = System.getenv("GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS");
      logger.atInfo().log("grpc env variable %s", grpcVariable);
      logger.atInfo().log("classpath for run %s", classpath);
      BenchmarkConfigurations benchmarkConfigurations = Args.commandLineParser(args);
      /*ReadResult readResult =
          ReadResult.builder()
              .setLibBufferSize(2 * 1024 * 1024)
              .setConnectorReadChunkSize(2 * 1024 * 1024)
              .build();

      JavaClientBenchmark javaClientBenchmark =
          new JavaClientBenchmark(benchmarkConfigurations, readResult);
      javaClientBenchmark.start();

      // using java-storage to hit gRPC-DIRECT-PATH
      Map<String, String> configMap = benchmarkConfigurations.getConfigMap();
      configMap.put("fs.gs.client.type", "STORAGE_CLIENT");
      configMap.put("fs.gs.grpc.enable", "true");
      configMap.put("fs.gs.grpc.trafficdirector.enable", "true");
      ReadBenchmark readBenchmark = new ReadBenchmark(benchmarkConfigurations, readResult);
      readBenchmark.start();

      // using gRPC library to hit gRPC-DIRECT-PATH
      configMap = benchmarkConfigurations.getConfigMap();
      configMap.put("fs.gs.client.type", "HTTP_API_CLIENT");
      configMap.put("fs.gs.grpc.enable", "true");
      configMap.put("fs.gs.grpc.trafficdirector.enable", "true");
      readBenchmark = new ReadBenchmark(benchmarkConfigurations, readResult);
      readBenchmark.start();

      // using Apiary json
      configMap = benchmarkConfigurations.getConfigMap();
      configMap.put("fs.gs.client.type", "HTTP_API_CLIENT");
      configMap.put("fs.gs.grpc.enable", "false");
      readBenchmark = new ReadBenchmark(benchmarkConfigurations, readResult);
      readBenchmark.start();
      readResult.close();*/

      // Write benchmark
      WriteResult writeResult = WriteResult.builder().setLibBufferSize(64 * 1024 * 1024).build();
      WriteBenchmark writeBenchmark = new WriteBenchmark(benchmarkConfigurations, writeResult);
      writeBenchmark.start();

      writeResult.close();

    } catch (IOException ioe) {
      logger.atWarning().withCause(ioe);
      throw ioe;
    }
  }
}
