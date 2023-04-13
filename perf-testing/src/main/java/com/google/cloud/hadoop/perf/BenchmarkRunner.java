package com.google.cloud.hadoop.perf;

import com.google.cloud.hadoop.perf.util.Args;
import com.google.cloud.hadoop.perf.util.BenchmarkConfigurations;
import com.google.cloud.hadoop.perf.util.BenchmarkConfigurations.BENCHMARK_TYPE_ENUM;
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
      if (benchmarkConfigurations.getBenchmarkType() == BENCHMARK_TYPE_ENUM.JAVA_STORAGE) {
        JavaClientBenchmark javaClientBenchmark = new JavaClientBenchmark(benchmarkConfigurations);
        javaClientBenchmark.start();
      } else {
        ReadBenchmark readBenchmark = new ReadBenchmark(benchmarkConfigurations);
        readBenchmark.start();
      }
    } catch (IOException ioe) {
      logger.atWarning().withCause(ioe);
      throw ioe;
    }
  }
}
