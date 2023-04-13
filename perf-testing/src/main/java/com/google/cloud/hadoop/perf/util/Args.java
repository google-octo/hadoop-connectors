package com.google.cloud.hadoop.perf.util;

import com.google.cloud.hadoop.perf.ConnectorConfigurations;
import com.google.cloud.hadoop.perf.util.BenchmarkConfigurations.BENCHMARK_TYPE_ENUM;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Args {

  public static BenchmarkConfigurations commandLineParser(String[] args) {
    CommandLine cmd;
    CommandLineParser parser = new BasicParser();
    Options options = getAllOptions();
    BenchmarkConfigurations benchmarkConfigurations = new BenchmarkConfigurations();
    try {
      cmd = parser.parse(options, args);

      if (cmd.hasOption("b")) {
        String benchmark_type = cmd.getOptionValue("b");
        switch (benchmark_type.toUpperCase()) {
          case "RANDOM_READ":
            benchmarkConfigurations.setBenchmarkType(BENCHMARK_TYPE_ENUM.RANDOM_READ);
            break;
          case "JAVA_STORAGE":
            benchmarkConfigurations.setBenchmarkType(BENCHMARK_TYPE_ENUM.JAVA_STORAGE);
          default:
            benchmarkConfigurations.setBenchmarkType(BENCHMARK_TYPE_ENUM.READ);
        }
      }
      if (cmd.hasOption("c")) {
        benchmarkConfigurations.setConfigMap(
            ConnectorConfigurations.getConfigMap(cmd.getOptionValue("c")));
      }

      if (cmd.hasOption("n")) {
        benchmarkConfigurations.setReadCalls(Integer.parseInt(cmd.getOptionValue("n")));
      }

    } catch (ParseException e) {
      throw new IllegalArgumentException(" provided options are not valid", e);
    }
    return benchmarkConfigurations;
  }

  public static Options getAllOptions() {
    Options options = new Options();
    Option benchmarkOption = new Option("b", "benchmarkType", true, "Type of benchmark");
    benchmarkOption.setRequired(true);
    options.addOption(benchmarkOption);
    options.addOption(
        new Option("c", "connectorConfig", true, "Config parameters to be passed to connector"));
    options.addOption(
        new Option("n", "callCount", true, "Total number of calls to be made for read or write."));
    return options;
  }
}
