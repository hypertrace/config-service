package ai.traceable.anomaly.config.service.detector.anomalydetection.handler;

import static ai.traceable.anomaly.config.service.common.AnomalyConfigServiceUtils.mergeConfigs;
import static ai.traceable.anomaly.config.service.v1.ApiProtectThreatRuleConfigMappingProvider.USER_ROLE_SPAN_FILTER_CONFIG;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyCategoryConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiProtectAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiProtectAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ApiProtectionConfigHandler {

  private static final Set<String> STRUCT_VALUE_CONFIGS_TO_REPLACE_DURING_MERGE =
      Set.of(USER_ROLE_SPAN_FILTER_CONFIG);

  /**
   * @param preferredConfig preferredConfig for merging
   * @param fallbackConfig fallbackConfig for merging
   * @return List of api-protection detection configs merged using ruleId as a key, the
   *     subRuleConfigs are merged on the basis of their subRuleId for a particular api-protection
   *     config
   */
  List<AnomalyDetectionConfig> merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    return merge(
        preferredConfig.getAnomalyDetectionConfigsList(),
        fallbackConfig.getAnomalyDetectionConfigsList());
  }

  public List<AnomalyDetectionConfig> merge(
      List<AnomalyDetectionConfig> preferredAnomalyDetectionConfigList,
      List<AnomalyDetectionConfig> fallbackAnomalyDetectionConfigList) {
    List<AnomalyDetectionConfig> preferredApiProtectionAnomalyRuleConfigs =
        getApiProtectConfigs(preferredAnomalyDetectionConfigList);
    List<AnomalyDetectionConfig> fallbackApiProtectionAnomalyRuleConfigs =
        getApiProtectConfigs(fallbackAnomalyDetectionConfigList);
    Map<String, AnomalyConfigStatusChange> configStatusMap =
        preferredApiProtectionAnomalyRuleConfigs.stream()
            .collect(
                Collectors.toMap(
                    detectionConfig ->
                        detectionConfig
                            .getApiProtectAnomalyDetectionConfig()
                            .getApiProtectAnomalyRule()
                            .getAnomalyRuleId(),
                    AnomalyDetectionConfig::getConfigStatus));

    fallbackApiProtectionAnomalyRuleConfigs.forEach(
        anomalyDetectionConfig -> {
          String ruleId =
              anomalyDetectionConfig
                  .getApiProtectAnomalyDetectionConfig()
                  .getApiProtectAnomalyRule()
                  .getAnomalyRuleId();
          if (!configStatusMap.containsKey(ruleId)) {
            configStatusMap.put(ruleId, anomalyDetectionConfig.getConfigStatus());
          } else {
            AnomalyConfigStatusChange mergedConfigStatusChange =
                (AnomalyConfigStatusChange)
                    mergeConfigs(
                        anomalyDetectionConfig.getConfigStatus(), configStatusMap.get(ruleId));
            configStatusMap.put(ruleId, mergedConfigStatusChange);
          }
        });

    Map<String, AnomalyCategoryConfig> configCategoryMap =
        preferredApiProtectionAnomalyRuleConfigs.stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getApiProtectAnomalyDetectionConfig()
                            .getApiProtectAnomalyRule()
                            .getAnomalyRuleId(),
                    AnomalyDetectionConfig::getCategoryConfig));

    fallbackApiProtectionAnomalyRuleConfigs.forEach(
        anomalyDetectionConfig -> {
          String ruleId =
              anomalyDetectionConfig
                  .getApiProtectAnomalyDetectionConfig()
                  .getApiProtectAnomalyRule()
                  .getAnomalyRuleId();
          if (!configCategoryMap.containsKey(ruleId)) {
            configCategoryMap.put(ruleId, anomalyDetectionConfig.getCategoryConfig());
          } else {
            AnomalyCategoryConfig mergedCategoryConfig =
                (AnomalyCategoryConfig)
                    mergeConfigs(
                        anomalyDetectionConfig.getCategoryConfig(), configCategoryMap.get(ruleId));
            configCategoryMap.put(ruleId, mergedCategoryConfig);
          }
        });

    Map<String, Map<String, AnomalySubRuleConfig>> apiProtectionConfigMap =
        mergeSubRuleConfigs(
            getApiProtectionAnomalyRuleConfigs(preferredApiProtectionAnomalyRuleConfigs),
            getApiProtectionAnomalyRuleConfigs(fallbackApiProtectionAnomalyRuleConfigs));

    List<AnomalyDetectionConfig> apiProtectionConfigs = new ArrayList<>();

    for (Map.Entry<String, Map<String, AnomalySubRuleConfig>> entry :
        apiProtectionConfigMap.entrySet()) {
      String anomalyRuleId = entry.getKey();
      AnomalyDetectionConfig.Builder builder = AnomalyDetectionConfig.newBuilder();
      builder.setCategoryConfig(
          configCategoryMap.getOrDefault(
              anomalyRuleId, AnomalyCategoryConfig.getDefaultInstance()));
      builder.setConfigStatus(
          configStatusMap.getOrDefault(
              anomalyRuleId, AnomalyConfigStatusChange.getDefaultInstance()));

      ApiProtectAnomalyRuleConfig.Builder apiProtectionConfigBuilder =
          ApiProtectAnomalyRuleConfig.newBuilder();
      apiProtectionConfigBuilder.setAnomalyRuleId(entry.getKey());
      for (Map.Entry<String, AnomalySubRuleConfig> subRuleConfigEntry :
          entry.getValue().entrySet()) {
        apiProtectionConfigBuilder.addSubRuleConfigs(subRuleConfigEntry.getValue());
      }

      builder.setApiProtectAnomalyDetectionConfig(
          ApiProtectAnomalyDetectionConfig.newBuilder()
              .setApiProtectAnomalyRule(apiProtectionConfigBuilder)
              .build());

      apiProtectionConfigs.add(builder.build());
    }

    return apiProtectionConfigs;
  }

  List<AnomalyDetectionConfig> deleteWholeAnomalyDetectionConfigs(
      List<AnomalyDetectionConfig> anomalyDetectionConfigs,
      List<AnomalyDetectionConfig> detectionConfigsToDelete,
      ScopedAnomalyDetectionConfig.Builder deletedConfigBuilder) {
    List<AnomalyDetectionConfig> filteredConfigs = new ArrayList<>();
    if (detectionConfigsToDelete.stream()
        .anyMatch(
            config ->
                config.hasApiProtectAnomalyDetectionConfig()
                    && config
                        .getApiProtectAnomalyDetectionConfig()
                        .equals(ApiProtectAnomalyDetectionConfig.getDefaultInstance()))) {
      anomalyDetectionConfigs.forEach(
          anomalyDetectionConfig -> {
            if (anomalyDetectionConfig.hasApiProtectAnomalyDetectionConfig()) {
              deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
            } else {
              filteredConfigs.add(anomalyDetectionConfig);
            }
          });
      return filteredConfigs;
    }

    Set<String> apiProtectionRuleIds =
        getApiProtectConfigs(detectionConfigsToDelete).stream()
            .map(
                detectionConfig ->
                    detectionConfig
                        .getApiProtectAnomalyDetectionConfig()
                        .getApiProtectAnomalyRule()
                        .getAnomalyRuleId())
            .collect(Collectors.toSet());

    for (AnomalyDetectionConfig anomalyDetectionConfig : anomalyDetectionConfigs) {
      if (anomalyDetectionConfig.getApiProtectAnomalyDetectionConfig().hasApiProtectAnomalyRule()
          && apiProtectionRuleIds.contains(
              anomalyDetectionConfig
                  .getApiProtectAnomalyDetectionConfig()
                  .getApiProtectAnomalyRule()
                  .getAnomalyRuleId())) {
        deletedConfigBuilder.addAnomalyDetectionConfigs(anomalyDetectionConfig);
      } else {
        filteredConfigs.add(anomalyDetectionConfig);
      }
    }
    return filteredConfigs;
  }

  private List<AnomalyDetectionConfig> getApiProtectConfigs(
      List<AnomalyDetectionConfig> detectionConfigs) {
    return detectionConfigs.stream()
        .filter(AnomalyDetectionConfig::hasApiProtectAnomalyDetectionConfig)
        .collect(Collectors.toList());
  }

  private Map<String, Map<String, AnomalySubRuleConfig>> mergeSubRuleConfigs(
      List<ApiProtectAnomalyRuleConfig> preferredConfigs,
      List<ApiProtectAnomalyRuleConfig> fallbackConfigs) {
    Map<String, Map<String, AnomalySubRuleConfig>> apiProtectSubRuleConfigMap =
        preferredConfigs.stream()
            .collect(
                Collectors.toMap(
                    ApiProtectAnomalyRuleConfig::getAnomalyRuleId,
                    this::getApiProtectSubRuleConfigMap));

    fallbackConfigs.forEach(
        apiProtectionConfig -> {
          String anomalyRuleId = apiProtectionConfig.getAnomalyRuleId();
          if (apiProtectSubRuleConfigMap.containsKey(anomalyRuleId)) {
            Map<String, AnomalySubRuleConfig> subRuleConfigMap =
                apiProtectSubRuleConfigMap.get(anomalyRuleId);
            apiProtectionConfig
                .getSubRuleConfigsList()
                .forEach(
                    anomalySubRuleConfig -> {
                      String subRuleId = anomalySubRuleConfig.getSubRuleId();
                      if (subRuleConfigMap.containsKey(subRuleId)) {
                        AnomalySubRuleConfig preferredSubRuleConfig =
                            subRuleConfigMap.get(subRuleId);
                        AnomalySubRuleConfig mergedAnomalySubRuleConfig =
                            (AnomalySubRuleConfig)
                                mergeConfigs(anomalySubRuleConfig, preferredSubRuleConfig);
                        mergedAnomalySubRuleConfig =
                            replaceConfigParams(mergedAnomalySubRuleConfig, preferredSubRuleConfig);
                        subRuleConfigMap.put(subRuleId, mergedAnomalySubRuleConfig);
                      } else {
                        subRuleConfigMap.put(subRuleId, anomalySubRuleConfig);
                      }
                    });

          } else {
            Map<String, AnomalySubRuleConfig> newSubRuleConfigMap = new HashMap<>();
            apiProtectionConfig
                .getSubRuleConfigsList()
                .forEach(
                    subRuleConfig ->
                        newSubRuleConfigMap.put(subRuleConfig.getSubRuleId(), subRuleConfig));
            apiProtectSubRuleConfigMap.put(anomalyRuleId, newSubRuleConfigMap);
          }
        });

    return apiProtectSubRuleConfigMap;
  }

  private Map<String, AnomalySubRuleConfig> getApiProtectSubRuleConfigMap(
      ApiProtectAnomalyRuleConfig apiProtectAnomalyRuleConfig) {
    return apiProtectAnomalyRuleConfig.getSubRuleConfigsList().stream()
        .collect(
            Collectors.toMap(
                AnomalySubRuleConfig::getSubRuleId, anomalySubRuleConfig -> anomalySubRuleConfig));
  }

  /**
   * After deep-merge, replace specific config_params values from the preferred config. Keys listed
   * in CONFIG_PARAMS_OVERRIDE_KEYS are fully replaced rather than deep-merged, preventing incorrect
   * accumulation of multiple oneof fields within Struct values.
   */
  private AnomalySubRuleConfig replaceConfigParams(
      AnomalySubRuleConfig mergedConfig, AnomalySubRuleConfig preferredConfig) {
    Map<String, Value> preferredParams = preferredConfig.getConfigParamsMap();
    if (preferredParams.isEmpty() || STRUCT_VALUE_CONFIGS_TO_REPLACE_DURING_MERGE.isEmpty()) {
      return mergedConfig;
    }
    AnomalySubRuleConfig.Builder builder = mergedConfig.toBuilder();
    STRUCT_VALUE_CONFIGS_TO_REPLACE_DURING_MERGE.stream()
        .filter(preferredParams::containsKey)
        .forEach(key -> builder.putConfigParams(key, preferredParams.get(key)));
    return builder.build();
  }

  private List<ApiProtectAnomalyRuleConfig> getApiProtectionAnomalyRuleConfigs(
      List<AnomalyDetectionConfig> anomalyDetectionConfigList) {
    return anomalyDetectionConfigList.stream()
        .filter(AnomalyDetectionConfig::hasApiProtectAnomalyDetectionConfig)
        .map(AnomalyDetectionConfig::getApiProtectAnomalyDetectionConfig)
        .filter(ApiProtectAnomalyDetectionConfig::hasApiProtectAnomalyRule)
        .map(ApiProtectAnomalyDetectionConfig::getApiProtectAnomalyRule)
        .collect(Collectors.toList());
  }
}
