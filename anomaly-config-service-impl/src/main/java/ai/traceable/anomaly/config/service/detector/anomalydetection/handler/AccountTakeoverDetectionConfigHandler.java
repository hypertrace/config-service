package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.registry.accounttakeover.AccountTakeoverRulesRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AccountTakeoverAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccountTakeoverDetectionConfigHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AccountTakeoverDetectionConfigHandler.class);

  private final Map<String, AccountTakeoverAnomalyDetectionConfig> accountTakeoverRuleIdToConfigMap;

  AccountTakeoverDetectionConfigHandler(AccountTakeoverRulesRegistry accountTakeoverRulesRegistry) {
    this.accountTakeoverRuleIdToConfigMap =
        accountTakeoverRulesRegistry.getAccountTakeoverRuleIdToConfigMap();
  }

  List<AnomalyDetectionConfig> merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    Map<String, AnomalyDetectionConfig> ruleIdToConfigMap = new HashMap<>();

    getAccountTakeoverConfigs(preferredConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig -> {
              AccountTakeoverAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getAccountTakeoverAnomalyDetectionConfig().getConfigCase();
              String ruleId =
                  detectionConfig.getAccountTakeoverAnomalyDetectionConfig().getAnomalyRuleId();
              if (configCase.equals(
                  AccountTakeoverAnomalyDetectionConfig.ConfigCase.CONFIG_NOT_SET)) {
                if (!accountTakeoverRuleIdToConfigMap.containsKey(ruleId)) {
                  LOGGER.error(
                      "Invalid ruleId \"{}\" and empty configCase for AccountTakeoverAnomalyDetectionConfig for configScope {}",
                      ruleId,
                      preferredConfig.getConfigScope());
                  return;
                }
                AccountTakeoverAnomalyDetectionConfig accountTakeoverAnomalyConfig =
                    accountTakeoverRuleIdToConfigMap.get(ruleId);
                detectionConfig =
                    (AnomalyDetectionConfig)
                        mergeConfigs(
                            detectionConfig,
                            AnomalyDetectionConfig.newBuilder()
                                .setAccountTakeoverAnomalyDetectionConfig(
                                    accountTakeoverAnomalyConfig)
                                .build());
              }
              ruleIdToConfigMap.put(ruleId, detectionConfig);
            });

    getAccountTakeoverConfigs(fallbackConfig.getAnomalyDetectionConfigsList())
        .forEach(
            detectionConfig -> {
              String ruleId =
                  detectionConfig.getAccountTakeoverAnomalyDetectionConfig().getAnomalyRuleId();
              if (ruleIdToConfigMap.containsKey(ruleId)) {
                AnomalyDetectionConfig mergedDetectionConfig =
                    (AnomalyDetectionConfig)
                        mergeConfigs(detectionConfig, ruleIdToConfigMap.get(ruleId));
                ruleIdToConfigMap.put(ruleId, mergedDetectionConfig);
              } else {
                ruleIdToConfigMap.put(ruleId, detectionConfig);
              }
            });
    return new ArrayList<>(ruleIdToConfigMap.values());
  }

  List<AnomalyDetectionConfig> deleteWholeAnomalyDetectionConfig(
      List<AnomalyDetectionConfig> anomalyDetectionConfigs,
      List<AnomalyDetectionConfig> detectionConfigsToDelete,
      ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder) {

    List<AnomalyDetectionConfig> filteredConfigs = new ArrayList<>();
    Set<AccountTakeoverAnomalyDetectionConfig.ConfigCase> accountTakeoverConfigCaseSet =
        getAccountTakeoverConfigs(detectionConfigsToDelete).stream()
            .map(
                detectionConfig ->
                    detectionConfig.getAccountTakeoverAnomalyDetectionConfig().getConfigCase())
            .collect(Collectors.toUnmodifiableSet());

    anomalyDetectionConfigs.forEach(
        detectionConfig -> {
          if (detectionConfig.hasAccountTakeoverAnomalyDetectionConfig()
              && accountTakeoverConfigCaseSet.contains(
                  detectionConfig.getAccountTakeoverAnomalyDetectionConfig().getConfigCase())) {
            deletedConfigBuilder.addAnomalyDetectionConfigs(detectionConfig);
          } else {
            filteredConfigs.add(detectionConfig);
          }
        });

    return filteredConfigs;
  }

  private List<AnomalyDetectionConfig> getAccountTakeoverConfigs(
      List<AnomalyDetectionConfig> anomalyDetectionConfigs) {
    return anomalyDetectionConfigs.stream()
        .filter(AnomalyDetectionConfig::hasAccountTakeoverAnomalyDetectionConfig)
        .collect(Collectors.toUnmodifiableList());
  }
}
