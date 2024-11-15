package ai.traceable.edge.config.service.config;

import ai.traceable.datamodel.data.transformation.config.v1.VariableDerivationMapping;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.SneakyThrows;
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

  private final ActorServiceConfig actorServiceConfig;
  private final Duration defaultAgentPollingFrequency;
  private final Map<String, EdgeConfigSupplierConfig> edgeConfigSupplierConfigs;
  private final Map<String, VariableDerivationMapping> defaultUserAttributionVariableRules;
  // TODO: Remove after adding proper conversion of user-attribution rules
  Map<String, List<String>> tenantUserIdBlockingConfigs;

  @Inject
  public TraceableEdgeConfig(Config config) {
    Config edgeConfig = config.getConfig(TRACEABLE_EDGE_CONFIG_SERVICE_NAME);
    this.actorServiceConfig = new ActorServiceConfig(config);

    this.defaultAgentPollingFrequency =
        getDuration(edgeConfig, DEFAULT_AGENT_POLLING_FREQUENCY_CONFIG_NAME);
    this.edgeConfigSupplierConfigs = extractEdgeConfigSupplierConfigs(edgeConfig);
    this.defaultUserAttributionVariableRules =
        extractDefaultUserAttributionVariableRules(edgeConfig);
    this.tenantUserIdBlockingConfigs = extractTenantUserIdBlockingConfigs(edgeConfig);
  }

  public Duration getAgentPollingFrequency(String configType) {
    return Optional.ofNullable(edgeConfigSupplierConfigs.get(configType))
        .map(EdgeConfigSupplierConfig::getAgentPollingFrequency)
        .orElse(defaultAgentPollingFrequency);
  }

  public ClientConfig getClientConfig() {
    return ClientConfig.DEFAULT;
  }

  public ActorServiceConfig getActorServiceConfig() {
    return actorServiceConfig;
  }

  public List<VariableDerivationMapping> getDefaultUserAttributionVariableRules(String tenantId) {
    return tenantUserIdBlockingConfigs.getOrDefault(tenantId, List.of()).stream()
        .map(defaultUserAttributionVariableRules::get)
        .filter(Objects::nonNull)
        .collect(Collectors.toUnmodifiableList());
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

  private Map<String, VariableDerivationMapping> extractDefaultUserAttributionVariableRules(
      Config config) {
    if (config.hasPath(DEFAULT_USER_ATTRIBUTION_VARIABLES_CONFIG_NAME)) {
      return config.getConfigList(DEFAULT_USER_ATTRIBUTION_VARIABLES_CONFIG_NAME).stream()
          .map(
              variableConfig -> {
                VariableDerivationMapping.Builder builder = VariableDerivationMapping.newBuilder();
                mergeFromConfig(variableConfig, builder);
                return builder.build();
              })
          .collect(Collectors.toMap(VariableDerivationMapping::getName, Function.identity()));
    }
    return Collections.emptyMap();
  }

  private Map<String, List<String>> extractTenantUserIdBlockingConfigs(Config config) {
    if (config.hasPath(USERID_BLOCKING_CONFIGS_CONFIG_NAME)) {
      return config.getConfigList(USERID_BLOCKING_CONFIGS_CONFIG_NAME).stream()
          .collect(
              Collectors.toMap(
                  blockingConfig -> blockingConfig.getString("tenantId"),
                  blockingConfig -> blockingConfig.getStringList("variables.user-attribution")));
    }
    return Collections.emptyMap();
  }

  @SneakyThrows
  private static void mergeFromConfig(Config config, Message.Builder builder) {
    JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
  }
}
