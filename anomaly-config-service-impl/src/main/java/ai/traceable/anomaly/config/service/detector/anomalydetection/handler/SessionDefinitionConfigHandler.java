package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SessionDefinitionConfigHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SessionDefinitionConfigHandler.class);
  private final Map<String, SessionDefinitionMetadataAnomalyDetectionConfig>
      sessionDefAnomalyDetectionConfigMap;

  SessionDefinitionConfigHandler(SessionRulesRegistry sessionRulesRegistry) {
    this.sessionDefAnomalyDetectionConfigMap =
        sessionRulesRegistry.getSessionDefRuleIdToDetectionConfigMap();
  }

  List<AnomalyDetectionConfig> merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    EnumMap<SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase, AnomalyDetectionConfig>
        configCaseMap =
            new EnumMap<>(SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase.class);

    getSessionDefConfigs(preferredConfig.getAnomalyDetectionConfigsList())
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
                    (AnomalyDetectionConfig)
                        mergeConfigs(
                            detectionConfig,
                            AnomalyDetectionConfig.newBuilder()
                                .setSessionDefinitionMetadataAnomalyDetectionConfig(
                                    sessionDefAnomalyConfig)
                                .build());
              }
              configCaseMap.put(configCase, detectionConfig);
            });

    getSessionDefConfigs(fallbackConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig -> {
              SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig
                      .getSessionDefinitionMetadataAnomalyDetectionConfig()
                      .getConfigCase();
              if (configCaseMap.containsKey(configCase)) {
                AnomalyDetectionConfig mergedDetectionConfig =
                    (AnomalyDetectionConfig)
                        mergeConfigs(detectionConfig, configCaseMap.get(configCase));
                configCaseMap.put(configCase, mergedDetectionConfig);
              } else {
                configCaseMap.put(configCase, detectionConfig);
              }
            });

    List<AnomalyDetectionConfig> resolvedConfigs = new ArrayList<>();
    resolvedConfigs.addAll(configCaseMap.values());

    return resolvedConfigs;
  }

  List<AnomalyDetectionConfig> deleteWholeAnomalyDetectionConfigs(
      List<AnomalyDetectionConfig> anomalyDetectionConfigs,
      List<AnomalyDetectionConfig> detectionConfigsToDelete,
      ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder) {
    List<AnomalyDetectionConfig> filteredConfigs = new ArrayList<>();
    if (detectionConfigsToDelete.stream()
        .anyMatch(
            config ->
                config.hasSessionDefinitionMetadataAnomalyDetectionConfig()
                    && config
                        .getSessionDefinitionMetadataAnomalyDetectionConfig()
                        .equals(
                            SessionDefinitionMetadataAnomalyDetectionConfig
                                .getDefaultInstance()))) {
      anomalyDetectionConfigs.forEach(
          anomalyDetectionConfig -> {
            if (anomalyDetectionConfig.hasSessionDefinitionMetadataAnomalyDetectionConfig()) {
              deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
            } else {
              filteredConfigs.add(anomalyDetectionConfig);
            }
          });
      return filteredConfigs;
    }
    Set<SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase> sessionDefConfigCases =
        getSessionDefConfigs(detectionConfigsToDelete).stream()
            .map(
                detectionConfig ->
                    detectionConfig
                        .getSessionDefinitionMetadataAnomalyDetectionConfig()
                        .getConfigCase())
            .collect(Collectors.toSet());

    for (AnomalyDetectionConfig anomalyDetectionConfig : anomalyDetectionConfigs) {
      if (anomalyDetectionConfig.hasSessionDefinitionMetadataAnomalyDetectionConfig()
          && sessionDefConfigCases.contains(
              anomalyDetectionConfig
                  .getSessionDefinitionMetadataAnomalyDetectionConfig()
                  .getConfigCase())) {
        deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
      } else {
        filteredConfigs.add(anomalyDetectionConfig);
      }
    }
    return filteredConfigs;
  }

  private List<AnomalyDetectionConfig> getSessionDefConfigs(
      List<AnomalyDetectionConfig> detectionConfigs) {
    return detectionConfigs.stream()
        .filter(AnomalyDetectionConfig::hasSessionDefinitionMetadataAnomalyDetectionConfig)
        .collect(Collectors.toList());
  }
}
