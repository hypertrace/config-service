package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;

import ai.traceable.anomaly.config.service.common.AnomalySubRuleConfigUtils;
import ai.traceable.anomaly.config.service.registry.accounttakeover.AccountTakeoverRulesRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AccountTakeoverAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccountTakeoverDetectionConfigHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AccountTakeoverDetectionConfigHandler.class);

  private final Map<String, AccountTakeoverAnomalyDetectionConfig> accountTakeoverRuleIdToConfigMap;
  private final String UNDER_SCORE = "_";

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
    return new ArrayList<>(
        populateNewFields(
            ruleIdToConfigMap.values(), preferredConfig.getAnomalyDetectionConfigsList()));
  }

  List<AnomalyDetectionConfig> deleteWholeAnomalyDetectionConfig(
      List<AnomalyDetectionConfig> anomalyDetectionConfigs,
      List<AnomalyDetectionConfig> detectionConfigsToDelete,
      ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder) {
    List<AnomalyDetectionConfig> filteredConfigs = new ArrayList<>();
    if (detectionConfigsToDelete.stream()
        .anyMatch(
            config ->
                config.hasAccountTakeoverAnomalyDetectionConfig()
                    && config
                        .getAccountTakeoverAnomalyDetectionConfig()
                        .equals(AccountTakeoverAnomalyDetectionConfig.getDefaultInstance()))) {
      anomalyDetectionConfigs.forEach(
          anomalyDetectionConfig -> {
            if (anomalyDetectionConfig.hasAccountTakeoverAnomalyDetectionConfig()) {
              deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
            } else {
              filteredConfigs.add(anomalyDetectionConfig);
            }
          });
      return filteredConfigs;
    }

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
                        .getAccountTakeoverAnomalyDetectionConfig()
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
                        .getAccountTakeoverAnomalyDetectionConfig()
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
              String ruleId = config.getAccountTakeoverAnomalyDetectionConfig().getAnomalyRuleId();
              Map<String, AnomalySubRuleConfig> ruleSpecificSubRules =
                  resultMap.entrySet().stream()
                      .filter(entry -> entry.getKey().startsWith(ruleId + UNDER_SCORE))
                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
              if (ruleSpecificSubRules.isEmpty()) {
                return config;
              }

              return AnomalyDetectionConfig.newBuilder(config)
                  .setAccountTakeoverAnomalyDetectionConfig(
                      config.getAccountTakeoverAnomalyDetectionConfig().toBuilder()
                          .setSubRuleConfigs(
                              config
                                  .getAccountTakeoverAnomalyDetectionConfig()
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
