package com.google.cloud.hadoop.perf;

import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BenchmarkRunner {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public static void main(String[] args) throws IOException {
    try {
      Map<String, String> configMap = new HashMap<>();
      if (args.length > 1) {
        configMap = ConnectorConfigurations.getConfigMap(args[0]);
      }
      ReadBenchmark readBenchmark = new ReadBenchmark(configMap);
      readBenchmark.runBenchmark();
    } catch (IOException ioe) {
      logger.atWarning().withCause(ioe);
      throw ioe;
    }
  }
}
