package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.registry.credentialstuffing.CredentialStuffingRulesRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.CredentialStuffingAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CredentialStuffingDetectionConfigHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CredentialStuffingDetectionConfigHandler.class);

  private final Map<String, CredentialStuffingAnomalyDetectionConfig>
      credentialStuffingRuleIdToConfigMap;

  CredentialStuffingDetectionConfigHandler(
      CredentialStuffingRulesRegistry credentialStuffingRulesRegistry) {
    this.credentialStuffingRuleIdToConfigMap =
        credentialStuffingRulesRegistry.getCredentialStuffingRuleIdToConfigMap();
  }

  List<AnomalyDetectionConfig> merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    EnumMap<CredentialStuffingAnomalyDetectionConfig.ConfigCase, AnomalyDetectionConfig>
        configCaseMap = new EnumMap<>(CredentialStuffingAnomalyDetectionConfig.ConfigCase.class);

    getCredentialStuffingConfigs(preferredConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig -> {
              CredentialStuffingAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getCredentialAnomalyDetectionConfig().getConfigCase();
              if (configCase.equals(
                  CredentialStuffingAnomalyDetectionConfig.ConfigCase.CONFIG_NOT_SET)) {
                String ruleId =
                    detectionConfig.getCredentialAnomalyDetectionConfig().getAnomalyRuleId();
                if (!credentialStuffingRuleIdToConfigMap.containsKey(ruleId)) {
                  LOGGER.error(
                      "Invalid ruleId \"{}\" and empty configCase for CredentialStuffingAnomalyDetectionConfig for configScope {}",
                      ruleId,
                      preferredConfig.getConfigScope());
                  return;
                }
                CredentialStuffingAnomalyDetectionConfig credentialStuffingAnomalyConfig =
                    credentialStuffingRuleIdToConfigMap.get(ruleId);
                configCase = credentialStuffingAnomalyConfig.getConfigCase();
                detectionConfig =
                    (AnomalyDetectionConfig)
                        mergeConfigs(
                            detectionConfig,
                            AnomalyDetectionConfig.newBuilder()
                                .setCredentialAnomalyDetectionConfig(
                                    credentialStuffingAnomalyConfig)
                                .build());
              }
              configCaseMap.put(configCase, detectionConfig);
            });

    getCredentialStuffingConfigs(fallbackConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig -> {
              CredentialStuffingAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getCredentialAnomalyDetectionConfig().getConfigCase();
              if (configCaseMap.containsKey(configCase)) {
                AnomalyDetectionConfig mergedDetectionConfig =
                    (AnomalyDetectionConfig)
                        mergeConfigs(configCaseMap.get(configCase), detectionConfig);
                configCaseMap.put(configCase, mergedDetectionConfig);
              } else {
                configCaseMap.put(configCase, detectionConfig);
              }
            });
    return new ArrayList<>(configCaseMap.values());
  }

  List<AnomalyDetectionConfig> deleteWholeAnomalyDetectionConfig(
      List<AnomalyDetectionConfig> anomalyDetectionConfigs,
      List<AnomalyDetectionConfig> detectionConfigsToDelete,
      ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder) {

    List<AnomalyDetectionConfig> filteredConfigs = new ArrayList<>();
    Set<CredentialStuffingAnomalyDetectionConfig.ConfigCase> credentialStuffingConfigCaseSet =
        getCredentialStuffingConfigs(detectionConfigsToDelete).stream()
            .map(
                detectionConfig ->
                    detectionConfig.getCredentialAnomalyDetectionConfig().getConfigCase())
            .collect(Collectors.toUnmodifiableSet());

    anomalyDetectionConfigs.forEach(
        detectionConfig -> {
          if (detectionConfig.hasCredentialAnomalyDetectionConfig()
              && credentialStuffingConfigCaseSet.contains(
                  detectionConfig.getCredentialAnomalyDetectionConfig().getConfigCase())) {
            deletedConfigBuilder.addAnomalyDetectionConfigs(detectionConfig);
          } else {
            filteredConfigs.add(detectionConfig);
          }
        });

    return filteredConfigs;
  }

  private List<AnomalyDetectionConfig> getCredentialStuffingConfigs(
      List<AnomalyDetectionConfig> anomalyDetectionConfigs) {
    return anomalyDetectionConfigs.stream()
        .filter(AnomalyDetectionConfig::hasCredentialAnomalyDetectionConfig)
        .collect(Collectors.toUnmodifiableList());
  }
}
