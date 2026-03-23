package ai.traceable.edge.config.service.config;

import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import com.google.protobuf.Duration;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
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
  private static final String GET_CONFIGS_THREAD_POOL_SIZE_CONFIG_NAME =
      "get.configs.thread.pool.size";
  private static final String CUSTOM_SIGNATURE_RULE_VERSION_CONFIG_NAME =
      "custom.signature.rule.version";
  private static final String AI_ENDPOINT_METADATA_ENABLED_CONFIG_NAME =
      "ai.endpoint.metadata.enabled";
  private static final int DEFAULT_GET_CONFIGS_EXECUTOR_SERVICE_THREAD_POOL_SIZE = 10;
  private static final CustomModsecRuleVersion DEFAULT_CUSTOM_SIGNATURE_RULE_VERSION =
      CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3;

  private final Duration defaultAgentPollingFrequency;
  private final Map<String, EdgeConfigSupplierConfig> edgeConfigSupplierConfigs;
  private final int getConfigsThreadPoolSize;
  private final CustomModsecRuleVersion customSignatureRuleVersion;
  @Getter public final boolean aiEndpointMetadataEnabled;

  @Inject
  public TraceableEdgeConfig(Config config) {
    Config edgeConfig = config.getConfig(TRACEABLE_EDGE_CONFIG_SERVICE_NAME);

    this.defaultAgentPollingFrequency =
        getDuration(edgeConfig, DEFAULT_AGENT_POLLING_FREQUENCY_CONFIG_NAME);
    this.edgeConfigSupplierConfigs = extractEdgeConfigSupplierConfigs(edgeConfig);
    this.getConfigsThreadPoolSize =
        edgeConfig.hasPath(GET_CONFIGS_THREAD_POOL_SIZE_CONFIG_NAME)
            ? edgeConfig.getInt(GET_CONFIGS_THREAD_POOL_SIZE_CONFIG_NAME)
            : DEFAULT_GET_CONFIGS_EXECUTOR_SERVICE_THREAD_POOL_SIZE;
    this.customSignatureRuleVersion = getCustomSignatureRuleVersion(edgeConfig);
    this.aiEndpointMetadataEnabled =
        edgeConfig.hasPath(AI_ENDPOINT_METADATA_ENABLED_CONFIG_NAME)
            && edgeConfig.getBoolean(AI_ENDPOINT_METADATA_ENABLED_CONFIG_NAME);
  }

  public int getConfigsThreadPoolSize() {
    return getConfigsThreadPoolSize;
  }

  public Duration getAgentPollingFrequency(String configType) {
    return Optional.ofNullable(edgeConfigSupplierConfigs.get(configType))
        .map(EdgeConfigSupplierConfig::getAgentPollingFrequency)
        .orElse(defaultAgentPollingFrequency);
  }

  public ClientConfig getClientConfig() {
    return ClientConfig.DEFAULT;
  }

  public CustomModsecRuleVersion getCustomSignatureRuleVersion() {
    return customSignatureRuleVersion;
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

  private CustomModsecRuleVersion getCustomSignatureRuleVersion(Config edgeConfig) {
    if (!edgeConfig.hasPath(CUSTOM_SIGNATURE_RULE_VERSION_CONFIG_NAME)) {
      return DEFAULT_CUSTOM_SIGNATURE_RULE_VERSION;
    }
    String configuredVersion = edgeConfig.getString(CUSTOM_SIGNATURE_RULE_VERSION_CONFIG_NAME);
    try {
      return CustomModsecRuleVersion.valueOf(configuredVersion);
    } catch (IllegalArgumentException ignored) {
      return DEFAULT_CUSTOM_SIGNATURE_RULE_VERSION;
    }
  }
}
