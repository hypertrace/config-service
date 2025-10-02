package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.common.AnomalySubRuleConfigUtils;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SessionDefinitionConfigHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SessionDefinitionConfigHandler.class);
  private final Map<String, SessionDefinitionMetadataAnomalyDetectionConfig>
      sessionDefAnomalyDetectionConfigMap;
  private final String UNDER_SCORE = "_";

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
    return new ArrayList<>(
        populateNewFields(
            configCaseMap.values(), preferredConfig.getAnomalyDetectionConfigsList()));
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

  private List<AnomalyDetectionConfig> populateNewFields(
      Collection<AnomalyDetectionConfig> mergedConfigs,
      List<AnomalyDetectionConfig> preferredConfigs) {
    if (mergedConfigs.isEmpty() || preferredConfigs.isEmpty()) {
      return new ArrayList<>(mergedConfigs);
    }

    Map<String, AnomalySubRuleConfig> preferredSubRuleConfigs =
        preferredConfigs.stream()
            .flatMap(
                config ->
                    config
                        .getSessionDefinitionMetadataAnomalyDetectionConfig()
                        .getSubRuleConfigs()
                        .getSubRuleConfigsMap()
                        .values()
                        .stream())
            .collect(Collectors.toMap(AnomalySubRuleConfig::getSubRuleId, Function.identity()));

    Map<String, AnomalySubRuleConfig> mergedSubRuleConfigs =
        mergedConfigs.stream()
            .flatMap(
                config ->
                    config
                        .getSessionDefinitionMetadataAnomalyDetectionConfig()
                        .getSubRuleConfigs()
                        .getSubRuleConfigsMap()
                        .values()
                        .stream())
            .collect(Collectors.toMap(AnomalySubRuleConfig::getSubRuleId, Function.identity()));
    Map<String, AnomalySubRuleConfig> resultMap = new HashMap<>();
    for (AnomalySubRuleConfig mergedConfig : mergedSubRuleConfigs.values()) {
      if (preferredSubRuleConfigs.get(mergedConfig.getSubRuleId()) != null) {
        AnomalySubRuleConfig preferredConfig =
            preferredSubRuleConfigs.get(mergedConfig.getSubRuleId());
        resultMap.put(
            mergedConfig.getSubRuleId(),
            AnomalySubRuleConfigUtils.handleMergedConfigChange(mergedConfig, preferredConfig));
      } else {
        resultMap.put(
            mergedConfig.getSubRuleId(), AnomalySubRuleConfigUtils.populateNewFields(mergedConfig));
      }
    }
    if (resultMap.isEmpty()) {
      return new ArrayList<>(mergedConfigs);
    }

    return mergedConfigs.stream()
        .map(
            config -> {
              String ruleId =
                  config.getSessionDefinitionMetadataAnomalyDetectionConfig().getAnomalyRuleId();
              Map<String, AnomalySubRuleConfig> ruleSpecificSubRules =
                  resultMap.entrySet().stream()
                      .filter(entry -> entry.getKey().startsWith(ruleId + UNDER_SCORE))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
              if (ruleSpecificSubRules.isEmpty()) {
                return config;
              }

              return AnomalyDetectionConfig.newBuilder(config)
                  .setSessionDefinitionMetadataAnomalyDetectionConfig(
                      config.getSessionDefinitionMetadataAnomalyDetectionConfig().toBuilder()
                          .setSubRuleConfigs(
                              config
                                  .getSessionDefinitionMetadataAnomalyDetectionConfig()
                                  .getSubRuleConfigs()
                                  .toBuilder()
                                  .clearSubRuleConfigs()
                                  .putAllSubRuleConfigs(ruleSpecificSubRules))
                          .build())
                  .build();
            })
        .collect(Collectors.toList());
  }
}
