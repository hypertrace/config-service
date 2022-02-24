package ai.traceable.anomaly.config.service.detector.anomalydetection.converter;

import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SessionDefinitionConfigConverter {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SessionDefinitionConfigConverter.class);
  private final Map<String, SessionDefinitionMetadataAnomalyDetectionConfig>
      sessionDefAnomalyDetectionConfigMap;

  SessionDefinitionConfigConverter(SessionRulesRegistry sessionRulesRegistry) {
    this.sessionDefAnomalyDetectionConfigMap =
        sessionRulesRegistry.getSessionDefRuleIdToDetectionConfigMap();
  }

  List<AnomalyDetectionConfig> merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    EnumMap<SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase, AnomalyDetectionConfig>
        configCaseMap =
            new EnumMap<>(SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase.class);

    preferredConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasSessionDefinitionMetadataAnomalyDetectionConfig)
        .forEach(
            detectionConfig -> {
              SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig
                      .getSessionDefinitionMetadataAnomalyDetectionConfig()
                      .getConfigCase();
              if (configCase.equals(
                  SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase.CONFIG_NOT_SET)) {
                String ruleId =
                    detectionConfig
                        .getSessionDefinitionMetadataAnomalyDetectionConfig()
                        .getAnomalyRuleId();
                if (!sessionDefAnomalyDetectionConfigMap.containsKey(ruleId)) {
                  LOGGER.error(
                      "Invalid ruleId \"{}\" and empty configCase for SessionDefinitionMetadataAnomalyDetectionConfig for configScope {}",
                      ruleId,
                      preferredConfig.getConfigScope());
                  return;
                }
                SessionDefinitionMetadataAnomalyDetectionConfig sessionDefAnomalyConfig =
                    sessionDefAnomalyDetectionConfigMap.get(ruleId);
                configCase = sessionDefAnomalyConfig.getConfigCase();
                detectionConfig =
                    detectionConfig.toBuilder()
                        .mergeFrom(
                            AnomalyDetectionConfig.newBuilder()
                                .setSessionDefinitionMetadataAnomalyDetectionConfig(
                                    sessionDefAnomalyConfig)
                                .build())
                        .build();
              }
              configCaseMap.put(configCase, detectionConfig);
            });

    fallbackConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasSessionDefinitionMetadataAnomalyDetectionConfig)
        .forEach(
            detectionConfig -> {
              SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig
                      .getSessionDefinitionMetadataAnomalyDetectionConfig()
                      .getConfigCase();
              if (configCaseMap.containsKey(configCase)) {
                configCaseMap.put(
                    configCase,
                    detectionConfig.toBuilder().mergeFrom(configCaseMap.get(configCase)).build());
              } else {
                configCaseMap.put(configCase, detectionConfig);
              }
            });

    List<AnomalyDetectionConfig> resolvedConfigs = new ArrayList<>();
    resolvedConfigs.addAll(configCaseMap.values());

    return resolvedConfigs;
  }
}
