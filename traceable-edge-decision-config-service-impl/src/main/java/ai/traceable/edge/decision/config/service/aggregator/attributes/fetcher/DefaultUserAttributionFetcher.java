package ai.traceable.edge.decision.config.service.aggregator.attributes.fetcher;

import ai.traceable.datamodel.data.transformation.config.v1.DataTransformationConfig;
import ai.traceable.datamodel.data.transformation.config.v1.DerivationRule;
import com.google.inject.Inject;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultUserAttributionFetcher implements UserAttributionFetcher {
  private final Map<String, List<DerivationRule>> tenantUserAttributionConfigs;
  private static final String DEFAULT_USER_ATTRIBUTION_CONFIG_PATH =
      "defaultTraceableEdgeUserAttributionVariables";
  private static final String DEFAULT_TRANSFORMATION_CONFIG_PATH = "transformation_config";

  // Want to throw error on unknown fields to avoid broad user attribution rules
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();

  @Inject
  public DefaultUserAttributionFetcher(Config config) {
    this.tenantUserAttributionConfigs = loadTenantConfigs(config);
  }

  public List<DerivationRule> getUserAttributionRules(String tenantId) {
    return tenantUserAttributionConfigs.getOrDefault(tenantId, Collections.emptyList());
  }

  /** Loads and parses the tenant-specific user attribution rules. */
  private Map<String, List<DerivationRule>> loadTenantConfigs(Config config) {
    if (!config.hasPath(DEFAULT_USER_ATTRIBUTION_CONFIG_PATH)) {
      log.warn(
          "No user attribution configurations found at path: {}",
          DEFAULT_USER_ATTRIBUTION_CONFIG_PATH);
      return Collections.emptyMap();
    }

    Config attributionConfig = config.getConfig(DEFAULT_USER_ATTRIBUTION_CONFIG_PATH);
    return attributionConfig.root().entrySet().stream()
        .collect(
            Collectors.toUnmodifiableMap(
                Map.Entry::getKey, // Tenant ID
                entry ->
                    convertRulesForTenant(
                        entry.getKey(), attributionConfig.getConfigList(entry.getKey()))));
  }

  /** Converts the rules for a specific tenant into DataTransformationConfig objects. */
  private List<DerivationRule> convertRulesForTenant(
      String tenantId, List<? extends Config> configList) {
    return configList.stream()
        .map(config -> convertDataTransformationConfig(tenantId, config))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(data -> DerivationRule.newBuilder().setTransformationConfig(data).build())
        .collect(Collectors.toUnmodifiableList());
  }

  /** Converts a single Config into a DataTransformationConfig, logging and skipping on failure. */
  private Optional<DataTransformationConfig> convertDataTransformationConfig(
      String tenantId, Config config) {
    try {
      Config transformationConfig = config.getConfig(DEFAULT_TRANSFORMATION_CONFIG_PATH);
      DataTransformationConfig.Builder builder = DataTransformationConfig.newBuilder();
      mergeFromConfig(transformationConfig, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      log.error(
          "Failed to convert default user-attribution rule for tenant {}: {}. Skipping this rule - {}",
          tenantId,
          e.getMessage(),
          config);
      return Optional.empty();
    }
  }

  @SneakyThrows
  private void mergeFromConfig(Config config, Message.Builder builder) {
    JSON_PARSER.merge(config.root().render(CONFIG_RENDER_CONCISE), builder);
  }
}
