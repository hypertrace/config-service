package ai.traceable.edge.config.service.config;

import com.google.protobuf.Duration;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ClientConfig;

public class TraceableEdgeConfig {
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();

  static final String TRACEABLE_EDGE_CONFIG_SERVICE_NAME = "traceable.edge.config.service";
  private static final String DEFAULT_AGENT_POLLING_FREQUENCY_CONFIG_NAME =
      "default.agent.polling.frequency";
  private static final String SUPPLIER_CONFIGS_CONFIG_NAME = "supplier.configs";
  private static final String DEFAULT_USER_ATTRIBUTION_VARIABLES_CONFIG_NAME =
      "default.variables.user-attribution";
  private static final String USERID_BLOCKING_CONFIGS_CONFIG_NAME = "userid.blocking.configs";

  private final Duration defaultAgentPollingFrequency;
  private final Map<String, EdgeConfigSupplierConfig> edgeConfigSupplierConfigs;

  @Inject
  public TraceableEdgeConfig(Config config) {
    Config edgeConfig = config.getConfig(TRACEABLE_EDGE_CONFIG_SERVICE_NAME);

    this.defaultAgentPollingFrequency =
        getDuration(edgeConfig, DEFAULT_AGENT_POLLING_FREQUENCY_CONFIG_NAME);
    this.edgeConfigSupplierConfigs = extractEdgeConfigSupplierConfigs(edgeConfig);
  }

  public Duration getAgentPollingFrequency(String configType) {
    return Optional.ofNullable(edgeConfigSupplierConfigs.get(configType))
        .map(EdgeConfigSupplierConfig::getAgentPollingFrequency)
        .orElse(defaultAgentPollingFrequency);
  }

  public ClientConfig getClientConfig() {
    return ClientConfig.DEFAULT;
  }

  private Map<String, EdgeConfigSupplierConfig> extractEdgeConfigSupplierConfigs(Config config) {
    if (config.hasPath(SUPPLIER_CONFIGS_CONFIG_NAME)) {
      return config.getConfigList(SUPPLIER_CONFIGS_CONFIG_NAME).stream()
          .map(
              supplierConfig ->
                  new EdgeConfigSupplierConfig(
                      supplierConfig.getString("configType"),
                      supplierConfig.getString("fullyQualifiedClassName"),
                      getDuration(supplierConfig, "agentPollingFrequency")))
          .collect(Collectors.toMap(EdgeConfigSupplierConfig::getConfigType, Function.identity()));
    }
    return Collections.emptyMap();
  }

  private Duration getDuration(Config config, String durationKey) {
    java.time.Duration configDuration = config.getDuration(durationKey);
    return Duration.newBuilder()
        .setSeconds(configDuration.getSeconds())
        .setNanos(configDuration.getNano())
        .build();
  }
}
