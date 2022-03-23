package ai.traceable.anomaly.config.service.aggregator.config;

import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.aggregator.AggregationConfig;
import ai.traceable.anomaly.config.service.v1.aggregator.EventAggregationGlobalConfig;
import com.typesafe.config.Config;
import java.util.Optional;
import javax.inject.Inject;

public class AggregationConfigServiceConfig {
  private static final String MODSEC_AGGREGATION_CONFIGS_PATH = "modsecAggregationConfig";
  private static final String API_DEFINITION_AGGREGATION_CONFIG_PATH =
      "apiDefinitionAggregationConfig";
  private static final String SESSION_AGGREGATION_CONFIG_PATH = "sessionAggregationConfig";
  private static final String GLOBAL_AGGREGATION_CONFIG_PATH = "globalAggregationConfig";
  private final Optional<AggregationConfig> modsecAggregationConfig;
  private final Optional<AggregationConfig> apiDefinitionAggregationConfig;
  private final Optional<AggregationConfig> sessionAggregationConfig;
  private final Optional<EventAggregationGlobalConfig> eventAggregationGlobalConfig;

  @Inject
  public AggregationConfigServiceConfig(Config config) {
    ConfigConverter configConverter = new ConfigConverter();
    this.modsecAggregationConfig =
        configConverter.getAggregationConfig(MODSEC_AGGREGATION_CONFIGS_PATH, config);
    this.apiDefinitionAggregationConfig =
        configConverter.getAggregationConfig(API_DEFINITION_AGGREGATION_CONFIG_PATH, config);
    this.sessionAggregationConfig =
        configConverter.getAggregationConfig(SESSION_AGGREGATION_CONFIG_PATH, config);
    this.eventAggregationGlobalConfig =
        configConverter.getGlobalAggregationConfig(GLOBAL_AGGREGATION_CONFIG_PATH, config);
  }

  public Optional<AggregationConfig> getDefaultModsecAggregationConfig() {
    return modsecAggregationConfig;
  }

  public Optional<AggregationConfig> getDefaultApiDefinitionAggregationConfig() {
    return apiDefinitionAggregationConfig;
  }

  public Optional<AggregationConfig> getDefaultSessionAggregationConfig() {
    return sessionAggregationConfig;
  }

  public Optional<EventAggregationGlobalConfig> getDefaultGlobalAggregationConfig() {
    return eventAggregationGlobalConfig;
  }
}
