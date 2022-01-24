package ai.traceable.anomaly.config.service.detector.anomalydetection;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyCategoryConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiStateBasedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

public class AnomalyDetectionConfigConverter {

  public Value convert(ScopedAnomalyDetectionConfig config) throws InvalidProtocolBufferException {
    return ConfigProtoConverter.convertToValue(config);
  }

  public ScopedAnomalyDetectionConfig convert(Value config) throws InvalidProtocolBufferException {
    ScopedAnomalyDetectionConfig.Builder builder = ScopedAnomalyDetectionConfig.newBuilder();

    if (config != null && config.getKindCase() != Value.KindCase.NULL_VALUE) {
      ConfigProtoConverter.mergeFromValue(config, builder);
    }
    return builder.build();
  }

  public ScopedAnomalyDetectionConfig merge(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {

    return ScopedAnomalyDetectionConfig.newBuilder()
        .setConfigScope(preferredConfig.getConfigScope())
        .addAllAnomalyDetectionConfigs(mergeModsecConfigs(preferredConfig, fallbackConfig))
        .addAllAnomalyDetectionConfigs(
            mergeStateBasedDetectionConfigs(preferredConfig, fallbackConfig))
        .addAllAnomalyDetectionConfigs(
            mergeApiDefinitionDetectionConfigs(preferredConfig, fallbackConfig))
        .build();
  }

  public Set<AnomalyDetectionConfig.AnomalyDetectionConfigCase> convert(
      GetAnomalyDetectionConfigsFilter filter) {

    Set<AnomalyDetectionConfig.AnomalyDetectionConfigCase> configCases = new HashSet<>();

    for (AnomalyDetectionConfigType configType : filter.getAnomalyDetectionConfigTypesList()) {
      switch (configType) {
        case ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY:
          configCases.add(
              AnomalyDetectionConfig.AnomalyDetectionConfigCase
                  .MODSECURITY_ANOMALY_DETECTION_CONFIG);
          break;
        case ANOMALY_DETECTION_CONFIG_TYPE_API_DEFINITION:
          configCases.add(
              AnomalyDetectionConfig.AnomalyDetectionConfigCase
                  .API_DEFINITION_METADATA_ANOMALY_DETECTION_CONFIG);
          break;
        case ANOMALY_DETECTION_CONFIG_TYPE_API_STATE_BASED:
          configCases.add(
              AnomalyDetectionConfig.AnomalyDetectionConfigCase
                  .API_STATE_BASED_ANOMALY_DETECTION_CONFIG);
          break;
        default:
          break;
      }
    }

    return configCases;
  }

  /**
   * @param preferredConfig
   * @param fallbackConfig
   * @return List of state based detection configs merged using config case as a key
   */
  private List<AnomalyDetectionConfig> mergeStateBasedDetectionConfigs(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    EnumMap<ApiStateBasedAnomalyDetectionConfig.ConfigCase, AnomalyDetectionConfig> configMap =
        new EnumMap<>(ApiStateBasedAnomalyDetectionConfig.ConfigCase.class);
    preferredConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasApiStateBasedAnomalyDetectionConfig)
        .forEach(
            detectionConfig ->
                configMap.put(
                    detectionConfig.getApiStateBasedAnomalyDetectionConfig().getConfigCase(),
                    detectionConfig));

    fallbackConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasApiStateBasedAnomalyDetectionConfig)
        .forEach(
            detectionConfig -> {
              ApiStateBasedAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getApiStateBasedAnomalyDetectionConfig().getConfigCase();
              if (configMap.containsKey(configCase)) {
                configMap.put(
                    configCase,
                    detectionConfig.toBuilder().mergeFrom(configMap.get(configCase)).build());
              } else {
                configMap.put(configCase, detectionConfig);
              }
            });

    return configMap.values().stream().collect(Collectors.toList());
  }

  /**
   * @param preferredConfig
   * @param fallbackConfig
   * @return List of api definition detection configs merged using ruleId as a key, in case ruleId
   *     is not present, the config case is used as a key for merging
   */
  private List<AnomalyDetectionConfig> mergeApiDefinitionDetectionConfigs(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    EnumMap<ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase, AnomalyDetectionConfig>
        configCaseMap = new EnumMap<>(ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.class);
    Map<String, AnomalyDetectionConfig> ruleIdMap = new HashMap<>();

    preferredConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasApiDefinitionMetadataAnomalyDetectionConfig)
        .forEach(
            detectionConfig -> {
              String ruleId =
                  detectionConfig
                      .getApiDefinitionMetadataAnomalyDetectionConfig()
                      .getAnomalyRuleId();
              if (ruleId.isEmpty()) {
                configCaseMap.put(
                    detectionConfig
                        .getApiDefinitionMetadataAnomalyDetectionConfig()
                        .getConfigCase(),
                    detectionConfig);
              } else {
                ruleIdMap.put(
                    detectionConfig
                        .getApiDefinitionMetadataAnomalyDetectionConfig()
                        .getAnomalyRuleId(),
                    detectionConfig);
              }
            });

    fallbackConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasApiDefinitionMetadataAnomalyDetectionConfig)
        .forEach(
            detectionConfig -> {
              ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getApiDefinitionMetadataAnomalyDetectionConfig().getConfigCase();
              String ruleId =
                  detectionConfig
                      .getApiDefinitionMetadataAnomalyDetectionConfig()
                      .getAnomalyRuleId();

              if (configCaseMap.containsKey(configCase)) {
                configCaseMap.put(
                    configCase,
                    detectionConfig.toBuilder().mergeFrom(configCaseMap.get(configCase)).build());
              } else if (ruleIdMap.containsKey(ruleId)) {
                ruleIdMap.put(
                    ruleId, detectionConfig.toBuilder().mergeFrom(ruleIdMap.get(ruleId)).build());
              } else {
                configCaseMap.put(configCase, detectionConfig);
              }
            });

    List<AnomalyDetectionConfig> resolvedConfigs = new ArrayList<>();
    resolvedConfigs.addAll(configCaseMap.values());
    resolvedConfigs.addAll(ruleIdMap.values());

    return resolvedConfigs;
  }

  /**
   * @param preferredConfig
   * @param fallbackConfig
   * @return List of modsec detection configs merged using ruleId as a key, the subRuleConfigs are
   *     merged on the basis of their subRuleId for a particular modsec config
   */
  private List<AnomalyDetectionConfig> mergeModsecConfigs(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    Map<String, AnomalyConfigStatusChange> configStatusMap =
        preferredConfig.getAnomalyDetectionConfigsList().stream()
            .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getModsecurityAnomalyDetectionConfig()
                            .getAnomalyRuleId(),
                    AnomalyDetectionConfig::getConfigStatus));

    fallbackConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
        .forEach(
            anomalyDetectionConfig -> {
              String ruleId =
                  anomalyDetectionConfig.getModsecurityAnomalyDetectionConfig().getAnomalyRuleId();
              if (!configStatusMap.containsKey(ruleId)) {
                configStatusMap.put(ruleId, anomalyDetectionConfig.getConfigStatus());
              } else {
                configStatusMap.put(
                    ruleId,
                    configStatusMap.get(ruleId).toBuilder()
                        .mergeFrom(anomalyDetectionConfig.getConfigStatus())
                        .build());
              }
            });

    Map<String, AnomalyCategoryConfig> configCategoryMap =
        preferredConfig.getAnomalyDetectionConfigsList().stream()
            .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getModsecurityAnomalyDetectionConfig()
                            .getAnomalyRuleId(),
                    AnomalyDetectionConfig::getCategoryConfig));

    fallbackConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
        .forEach(
            anomalyDetectionConfig -> {
              String ruleId =
                  anomalyDetectionConfig.getModsecurityAnomalyDetectionConfig().getAnomalyRuleId();
              if (!configCategoryMap.containsKey(ruleId)) {
                configCategoryMap.put(ruleId, anomalyDetectionConfig.getCategoryConfig());
              } else {
                configCategoryMap.put(
                    ruleId,
                    configCategoryMap.get(ruleId).toBuilder()
                        .mergeFrom(anomalyDetectionConfig.getCategoryConfig())
                        .build());
              }
            });

    Map<String, Map<String, AnomalySubRuleConfig>> modsecConfigMap =
        mergeSubRuleConfigs(getModsecConfigs(preferredConfig), getModsecConfigs(fallbackConfig));

    List<AnomalyDetectionConfig> modsecConfigs = new ArrayList<>();

    for (Map.Entry<String, Map<String, AnomalySubRuleConfig>> entry : modsecConfigMap.entrySet()) {
      String anomalyRuleId = entry.getKey();
      AnomalyDetectionConfig.Builder builder = AnomalyDetectionConfig.newBuilder();
      builder.setCategoryConfig(configCategoryMap.get(anomalyRuleId));
      builder.setConfigStatus(configStatusMap.get(anomalyRuleId));

      ModsecurityAnomalyDetectionConfig.Builder modsecConfigBuilder =
          ModsecurityAnomalyDetectionConfig.newBuilder();
      modsecConfigBuilder.setAnomalyRuleId(entry.getKey());
      for (Map.Entry<String, AnomalySubRuleConfig> subRuleConfigEntry :
          entry.getValue().entrySet()) {
        modsecConfigBuilder.addSubRuleConfigs(subRuleConfigEntry.getValue());
      }

      builder.setModsecurityAnomalyDetectionConfig(modsecConfigBuilder);

      modsecConfigs.add(builder.build());
    }

    return modsecConfigs;
  }

  private Map<String, Map<String, AnomalySubRuleConfig>> mergeSubRuleConfigs(
      List<ModsecurityAnomalyDetectionConfig> preferredConfigs,
      List<ModsecurityAnomalyDetectionConfig> fallbackConfigs) {
    Map<String, Map<String, AnomalySubRuleConfig>> modsecSubRuleConfigMap =
        preferredConfigs.stream()
            .collect(
                Collectors.toMap(
                    ModsecurityAnomalyDetectionConfig::getAnomalyRuleId,
                    this::getModsecSubRuleConfigMap));

    fallbackConfigs.forEach(
        modsecConfig -> {
          String anomalyRuleId = modsecConfig.getAnomalyRuleId();
          if (modsecSubRuleConfigMap.containsKey(anomalyRuleId)) {
            Map<String, AnomalySubRuleConfig> subRuleConfigMap =
                modsecSubRuleConfigMap.get(anomalyRuleId);
            modsecConfig
                .getSubRuleConfigsList()
                .forEach(
                    anomalySubRuleConfig -> {
                      String subRuleId = anomalySubRuleConfig.getSubRuleId();
                      if (subRuleConfigMap.containsKey(subRuleId)) {
                        subRuleConfigMap.put(
                            subRuleId,
                            anomalySubRuleConfig.toBuilder()
                                .mergeFrom(subRuleConfigMap.get(subRuleId))
                                .build());
                      } else {
                        subRuleConfigMap.put(subRuleId, anomalySubRuleConfig);
                      }
                    });

          } else {
            modsecSubRuleConfigMap.put(anomalyRuleId, getModsecSubRuleConfigMap(modsecConfig));
          }
        });

    return modsecSubRuleConfigMap;
  }

  private Map<String, AnomalySubRuleConfig> getModsecSubRuleConfigMap(
      ModsecurityAnomalyDetectionConfig modsecurityAnomalyDetectionConfig) {
    return modsecurityAnomalyDetectionConfig.getSubRuleConfigsList().stream()
        .collect(
            Collectors.toMap(
                AnomalySubRuleConfig::getSubRuleId, anomalySubRuleConfig -> anomalySubRuleConfig));
  }

  private List<ModsecurityAnomalyDetectionConfig> getModsecConfigs(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig) {
    return scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
        .map(AnomalyDetectionConfig::getModsecurityAnomalyDetectionConfig)
        .collect(Collectors.toList());
  }
}
