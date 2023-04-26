package com.google.cloud.hadoop.perf;

import com.google.common.flogger.GoogleLogger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;

public class ConnectorConfigurations {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  protected ConnectorConfigurations() {}

  public static Map<String, String> getConfigMap(String config) {
    if (config == null || config == "") {
      return Collections.emptyMap();
    }
    Map<String, String> configMap = new HashMap<>();
    String[] configValue = config.split(",");
    for (String s : configValue) {
      String[] c = s.split("=");
      if (c.length != 2) {
        String msg = String.format("Wrong config value provided, %s", s);
        logger.atSevere().log(msg);
        throw new IllegalArgumentException(msg);
      }
      configMap.put(c[0], c[1]);
    }

    return configMap;
  }

  protected Configuration getDefaultConfiguration() {
    Configuration configuration = new Configuration();
    return configuration;
  }

  protected Configuration getConfigurationFromMap(Map<String, String> configMap) {
    Configuration configuration = getDefaultConfiguration();
    for (Entry<String, String> config : configMap.entrySet()) {
      configuration.set(config.getKey(), config.getValue());
    }
    return configuration;
  }
}
