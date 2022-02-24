package ai.traceable.anomaly.config.service.detector.anomalydetection;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyCategoryConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiStateBasedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.BlockingMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnomalyDetectionConfigConverter {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AnomalyDetectionConfigConverter.class);
  private final Map<String, ApiDefinitionMetadataAnomalyDetectionConfig>
      apiDefMetadataAnomalyDetectionConfigMap;
  private final Map<String, SessionDefinitionMetadataAnomalyDetectionConfig>
      sessionDefAnomalyDetectionConfigMap;

  @Inject
  public AnomalyDetectionConfigConverter(
      ApiDefinitionRegistry apiDefinitionRegistry, SessionRulesRegistry sessionRulesRegistry) {
    this.apiDefMetadataAnomalyDetectionConfigMap =
        apiDefinitionRegistry.getApiDefRuleIdToDetectionConfigMap();
    this.sessionDefAnomalyDetectionConfigMap =
        sessionRulesRegistry.getSessionDefRuleIdToDetectionConfigMap();
  }

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
        .addAllAnomalyDetectionConfigs(
            mergeBlockingDetectionConfigs(preferredConfig, fallbackConfig))
        .addAllAnomalyDetectionConfigs(
            mergeSessionDefinitionMetadataConfigs(preferredConfig, fallbackConfig))
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
        case ANOMALY_DETECTION_CONFIG_TYPE_SESSION_DEFINITION:
          configCases.add(
              AnomalyDetectionConfig.AnomalyDetectionConfigCase
                  .SESSION_DEFINITION_METADATA_ANOMALY_DETECTION_CONFIG);
          break;
        case ANOMALY_DETECTION_CONFIG_TYPE_BLOCKING_METADATA:
          configCases.add(
              AnomalyDetectionConfig.AnomalyDetectionConfigCase
                  .BLOCKING_METADATA_ANOMALY_DETECTION_CONFIG);
          break;
        default:
          break;
      }
    }

    return configCases;
  }

  private List<AnomalyDetectionConfig> mergeSessionDefinitionMetadataConfigs(
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
                      "Invalid ruleId {} for SessionDefinitionMetadataAnomalyDetectionConfig",
                      ruleId);
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

  /**
   * @param preferredConfig
   * @param fallbackConfig
   * @return List of blocking detection configs merged using config case as a key
   */
  private List<AnomalyDetectionConfig> mergeBlockingDetectionConfigs(
      ScopedAnomalyDetectionConfig preferredConfig, ScopedAnomalyDetectionConfig fallbackConfig) {
    EnumMap<BlockingMetadataAnomalyDetectionConfig.ConfigCase, AnomalyDetectionConfig> configMap =
        new EnumMap<>(BlockingMetadataAnomalyDetectionConfig.ConfigCase.class);

    preferredConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasBlockingMetadataAnomalyDetectionConfig)
        .forEach(
            detectionConfig ->
                configMap.put(
                    detectionConfig.getBlockingMetadataAnomalyDetectionConfig().getConfigCase(),
                    detectionConfig));

    fallbackConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasBlockingMetadataAnomalyDetectionConfig)
        .forEach(
            detectionConfig -> {
              BlockingMetadataAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getBlockingMetadataAnomalyDetectionConfig().getConfigCase();
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

    preferredConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasApiDefinitionMetadataAnomalyDetectionConfig)
        .forEach(
            detectionConfig -> {
              ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getApiDefinitionMetadataAnomalyDetectionConfig().getConfigCase();
              if (configCase.equals(
                  ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.CONFIG_NOT_SET)) {
                String ruleId =
                    detectionConfig
                        .getApiDefinitionMetadataAnomalyDetectionConfig()
                        .getAnomalyRuleId();
                if (!apiDefMetadataAnomalyDetectionConfigMap.containsKey(ruleId)) {
                  LOGGER.error(
                      "Invalid ruleId {} for ApiDefinitionMetadataAnomalyDetectionConfig", ruleId);
                  return;
                }
                ApiDefinitionMetadataAnomalyDetectionConfig apiDefMetadataAnomalyConfig =
                    apiDefMetadataAnomalyDetectionConfigMap.get(
                        detectionConfig
                            .getApiDefinitionMetadataAnomalyDetectionConfig()
                            .getAnomalyRuleId());
                configCase = apiDefMetadataAnomalyConfig.getConfigCase();
                detectionConfig =
                    detectionConfig.toBuilder()
                        .mergeFrom(
                            AnomalyDetectionConfig.newBuilder()
                                .setApiDefinitionMetadataAnomalyDetectionConfig(
                                    apiDefMetadataAnomalyConfig)
                                .build())
                        .build();
              }
              configCaseMap.put(configCase, detectionConfig);
            });

    fallbackConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasApiDefinitionMetadataAnomalyDetectionConfig)
        .forEach(
            detectionConfig -> {
              ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase =
                  detectionConfig.getApiDefinitionMetadataAnomalyDetectionConfig().getConfigCase();
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
            .filter(
                detectionConfig ->
                    detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAnomalyRule())
            .collect(
                Collectors.toMap(
                    detectionConfig ->
                        detectionConfig
                            .getModsecurityAnomalyDetectionConfig()
                            .getModsecAnomalyRule()
                            .getAnomalyRuleId(),
                    AnomalyDetectionConfig::getConfigStatus));

    fallbackConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
        .filter(
            detectionConfig ->
                detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAnomalyRule())
        .forEach(
            anomalyDetectionConfig -> {
              String ruleId =
                  anomalyDetectionConfig
                      .getModsecurityAnomalyDetectionConfig()
                      .getModsecAnomalyRule()
                      .getAnomalyRuleId();
              if (!configStatusMap.containsKey(ruleId)) {
                configStatusMap.put(ruleId, anomalyDetectionConfig.getConfigStatus());
              } else {
                configStatusMap.put(
                    ruleId,
                    anomalyDetectionConfig.getConfigStatus().toBuilder()
                        .mergeFrom(configStatusMap.get(ruleId))
                        .build());
              }
            });

    Map<String, AnomalyCategoryConfig> configCategoryMap =
        preferredConfig.getAnomalyDetectionConfigsList().stream()
            .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
            .filter(
                detectionConfig ->
                    detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAnomalyRule())
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getModsecurityAnomalyDetectionConfig()
                            .getModsecAnomalyRule()
                            .getAnomalyRuleId(),
                    AnomalyDetectionConfig::getCategoryConfig));

    fallbackConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
        .filter(
            detectionConfig ->
                detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAnomalyRule())
        .forEach(
            anomalyDetectionConfig -> {
              String ruleId =
                  anomalyDetectionConfig
                      .getModsecurityAnomalyDetectionConfig()
                      .getModsecAnomalyRule()
                      .getAnomalyRuleId();
              if (!configCategoryMap.containsKey(ruleId)) {
                configCategoryMap.put(ruleId, anomalyDetectionConfig.getCategoryConfig());
              } else {
                configCategoryMap.put(
                    ruleId,
                    anomalyDetectionConfig.getCategoryConfig().toBuilder()
                        .mergeFrom(configCategoryMap.get(ruleId))
                        .build());
              }
            });

    Map<String, Map<String, AnomalySubRuleConfig>> modsecConfigMap =
        mergeSubRuleConfigs(
            getModsecAnomalyRuleConfigs(preferredConfig),
            getModsecAnomalyRuleConfigs(fallbackConfig));

    List<AnomalyDetectionConfig> modsecConfigs = new ArrayList<>();

    for (Map.Entry<String, Map<String, AnomalySubRuleConfig>> entry : modsecConfigMap.entrySet()) {
      String anomalyRuleId = entry.getKey();
      AnomalyDetectionConfig.Builder builder = AnomalyDetectionConfig.newBuilder();
      builder.setCategoryConfig(
          configCategoryMap.getOrDefault(
              anomalyRuleId, AnomalyCategoryConfig.getDefaultInstance()));
      builder.setConfigStatus(
          configStatusMap.getOrDefault(
              anomalyRuleId, AnomalyConfigStatusChange.getDefaultInstance()));

      ModsecurityAnomalyRuleConfig.Builder modsecConfigBuilder =
          ModsecurityAnomalyRuleConfig.newBuilder();
      modsecConfigBuilder.setAnomalyRuleId(entry.getKey());
      for (Map.Entry<String, AnomalySubRuleConfig> subRuleConfigEntry :
          entry.getValue().entrySet()) {
        modsecConfigBuilder.addSubRuleConfigs(subRuleConfigEntry.getValue());
      }

      builder.setModsecurityAnomalyDetectionConfig(
          ModsecurityAnomalyDetectionConfig.newBuilder().setModsecAnomalyRule(modsecConfigBuilder));

      modsecConfigs.add(builder.build());
    }

    AnomalyDetectionConfig modsecurityAllDetectionConfig =
        AnomalyDetectionConfig.getDefaultInstance();

    for (AnomalyDetectionConfig detectionConfig : fallbackConfig.getAnomalyDetectionConfigsList()) {
      if (detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAllDetection()) {
        modsecurityAllDetectionConfig = detectionConfig;
        break;
      }
    }

    for (AnomalyDetectionConfig detectionConfig :
        preferredConfig.getAnomalyDetectionConfigsList()) {
      if (detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAllDetection()) {
        modsecurityAllDetectionConfig =
            modsecurityAllDetectionConfig.toBuilder().mergeFrom(detectionConfig).build();
        break;
      }
    }

    if (!modsecurityAllDetectionConfig.equals(AnomalyDetectionConfig.getDefaultInstance())) {
      modsecConfigs.add(modsecurityAllDetectionConfig);
    }

    return modsecConfigs;
  }

  private Map<String, Map<String, AnomalySubRuleConfig>> mergeSubRuleConfigs(
      List<ModsecurityAnomalyRuleConfig> preferredConfigs,
      List<ModsecurityAnomalyRuleConfig> fallbackConfigs) {
    Map<String, Map<String, AnomalySubRuleConfig>> modsecSubRuleConfigMap =
        preferredConfigs.stream()
            .collect(
                Collectors.toMap(
                    ModsecurityAnomalyRuleConfig::getAnomalyRuleId,
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
      ModsecurityAnomalyRuleConfig modsecurityAnomalyDetectionConfig) {
    return modsecurityAnomalyDetectionConfig.getSubRuleConfigsList().stream()
        .collect(
            Collectors.toMap(
                AnomalySubRuleConfig::getSubRuleId, anomalySubRuleConfig -> anomalySubRuleConfig));
  }

  private List<ModsecurityAnomalyRuleConfig> getModsecAnomalyRuleConfigs(
      ScopedAnomalyDetectionConfig scopedAnomalyDetectionConfig) {
    return scopedAnomalyDetectionConfig.getAnomalyDetectionConfigsList().stream()
        .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
        .map(AnomalyDetectionConfig::getModsecurityAnomalyDetectionConfig)
        .filter(ModsecurityAnomalyDetectionConfig::hasModsecAnomalyRule)
        .map(ModsecurityAnomalyDetectionConfig::getModsecAnomalyRule)
        .collect(Collectors.toList());
  }
}
