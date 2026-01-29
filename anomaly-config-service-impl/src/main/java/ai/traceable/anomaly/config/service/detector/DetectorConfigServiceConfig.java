package ai.traceable.anomaly.config.service.detector;

import ai.traceable.anomaly.config.service.registry.accounttakeover.AccountTakeoverRulesRegistry;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.registry.credentialstuffing.CredentialStuffingRulesRegistry;
import ai.traceable.anomaly.config.service.registry.genai.GenAiRulesRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistry;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DetectorConfigServiceConfig {
  private static final String MODSEC_DETECTION_CONFIGS_PATH = "modsecDetectionConfigs";
  private static final String API_PROTECT_DETECTION_DEFAULT_CONFIGS_PATH =
      "api-protect-threat-rule-configs.conf";
  private static final String API_PROTECT_DETECTION_CONFIGS_PATH =
      "apiProtectThreatRuleIdToConfigMap";
  private static final String API_PROTECT_TAGGED_CATEGORY_RULE_IDS_PATH =
      "apiProtectTaggedCategoryRuleIds";
  private static final String API_DEFINITION_DETECTION_CONFIGS_PATH =
      "apiDefinitionDetectionConfigs";
  private static final String SESSION_DEFINITION_DETECTION_CONFIGS_PATH =
      "sessionDefinitionDetectionConfigs";
  private static final String CUSTOM_RULES_DETECTION_CONFIGS_PATH = "customRulesDetectionConfigs";
  private static final String GEN_AI_DETECTION_CONFIGS_PATH = "genAiDetectionConfigs";
  private static final String VOLUMETRIC_DETECTION_CONFIGS_PATH = "volumetricDetectionConfigs";
  private static final String CREDENTIAL_STUFFING_DETECTION_CONFIGS_PATH =
      "credentialStuffingDetectionConfigs";
  private static final String ACCOUNT_TAKEOVER_DETECTION_CONFIGS_PATH =
      "accountTakeoverDetectionConfigs";

  private final List<AnomalyDetectionConfig> wafDetectionConfigs;
  private final List<AnomalyDetectionConfig> apiProtectDetectionConfigs;
  private final List<AnomalyDetectionConfig> deprecatedApiProtectionDetectionConfigs;
  private final List<AnomalyDetectionConfig> genAiDetectionConfigs;
  private final List<String> apiProtectTaggedCategoryRuleIds;

  private final ConfigConverter configConverter = new ConfigConverter();

  public DetectorConfigServiceConfig(
      Config config,
      ApiDefinitionRegistry apiDefinitionRegistry,
      SessionRulesRegistry sessionDefinitionRegistry,
      VolumetricRulesRegistry volumetricRulesRegistry,
      CredentialStuffingRulesRegistry credentialStuffingRulesRegistry,
      AccountTakeoverRulesRegistry accountTakeoverRulesRegistry,
      GenAiRulesRegistry genAiRulesRegistry) {
    this.apiProtectTaggedCategoryRuleIds = loadApiProtectTaggedCategoryRuleIds(config);
    this.apiProtectDetectionConfigs = loadApiProtectDetectionConfigs(config);
    this.wafDetectionConfigs =
        configConverter.convertToAnomalyDetectionConfigs(
            config.getConfigList(MODSEC_DETECTION_CONFIGS_PATH));
    this.genAiDetectionConfigs =
        loadDefaultGenAiDetectionConfigs(
            config,
            genAiRulesRegistry.getGenAiRuleIdToConfigMap().values().stream()
                .map(
                    detectionConfig ->
                        AnomalyDetectionConfig.newBuilder()
                            .setGenAiAnomalyDetectionConfig(detectionConfig)
                            .build())
                .collect(Collectors.toList()));

    List<AnomalyDetectionConfig> deprecatedApiProtectionDetectionConfigs = new ArrayList<>();
    deprecatedApiProtectionDetectionConfigs.addAll(
        loadDefaultApiDefinitionDetectionConfigs(
            config,
            apiDefinitionRegistry.getApiDefRuleIdToDetectionConfigMap().values().stream()
                .map(
                    detectionConfig ->
                        AnomalyDetectionConfig.newBuilder()
                            .setApiDefinitionMetadataAnomalyDetectionConfig(detectionConfig)
                            .build())
                .collect(Collectors.toList())));
    deprecatedApiProtectionDetectionConfigs.addAll(
        loadDefaultSessionDefinitionDetectionConfigs(
            config,
            sessionDefinitionRegistry.getSessionDefRuleIdToDetectionConfigMap().values().stream()
                .map(
                    detectionConfig ->
                        AnomalyDetectionConfig.newBuilder()
                            .setSessionDefinitionMetadataAnomalyDetectionConfig(detectionConfig)
                            .build())
                .collect(Collectors.toList())));
    deprecatedApiProtectionDetectionConfigs.addAll(
        configConverter.convertToAnomalyDetectionConfigs(
            config.getConfigList(CUSTOM_RULES_DETECTION_CONFIGS_PATH)));

    deprecatedApiProtectionDetectionConfigs.addAll(
        loadDefaultVolumetricDetectionConfigs(
            config,
            volumetricRulesRegistry.getVolumetricRuleIdToConfigMap().values().stream()
                .map(
                    detectionConfig ->
                        AnomalyDetectionConfig.newBuilder()
                            .setVolumetricAnomalyDetectionConfig(detectionConfig)
                            .build())
                .collect(Collectors.toList())));
    deprecatedApiProtectionDetectionConfigs.addAll(
        loadDefaultCredentialStuffingDetectionConfigs(
            config,
            credentialStuffingRulesRegistry
                .getCredentialStuffingRuleIdToConfigMap()
                .values()
                .stream()
                .map(
                    detectionConfig ->
                        AnomalyDetectionConfig.newBuilder()
                            .setCredentialAnomalyDetectionConfig(detectionConfig)
                            .build())
                .collect(Collectors.toList())));
    deprecatedApiProtectionDetectionConfigs.addAll(
        loadDefaultAccountTakeoverDetectionConfigs(
            config,
            accountTakeoverRulesRegistry.getAccountTakeoverRuleIdToConfigMap().values().stream()
                .map(
                    detectionConfig ->
                        AnomalyDetectionConfig.newBuilder()
                            .setAccountTakeoverAnomalyDetectionConfig(detectionConfig)
                            .build())
                .collect(Collectors.toList())));
    this.deprecatedApiProtectionDetectionConfigs = deprecatedApiProtectionDetectionConfigs;
  }

  private List<String> loadApiProtectTaggedCategoryRuleIds(Config config) {
    if (config == null || !config.hasPath(API_PROTECT_TAGGED_CATEGORY_RULE_IDS_PATH)) {
      return new ArrayList<>();
    }
    return config.getStringList(API_PROTECT_TAGGED_CATEGORY_RULE_IDS_PATH);
  }

  public List<AnomalyDetectionConfig> getDefaultWafDetectionConfigs() {
    return wafDetectionConfigs;
  }

  public List<AnomalyDetectionConfig> getDefaultApiProtectDetectionConfigs() {
    return apiProtectDetectionConfigs;
  }

  public List<String> getApiProtectTaggedCategoryRuleIds() {
    return apiProtectTaggedCategoryRuleIds;
  }

  public List<AnomalyDetectionConfig> getDefaultGenAiDetectionConfigs() {
    return genAiDetectionConfigs;
  }

  public List<AnomalyDetectionConfig> getDefaultApiProtectionDetectionConfigs() {
    return deprecatedApiProtectionDetectionConfigs;
  }

  private List<AnomalyDetectionConfig> loadApiProtectDetectionConfigs(Config config) {
    try {
      if (config != null && config.hasPath(API_PROTECT_DETECTION_CONFIGS_PATH)) {
        return configConverter.convertToAnomalyDetectionConfigs(
            config.getConfigList(API_PROTECT_DETECTION_CONFIGS_PATH));
      }

      Config apiProtectConfig =
          ConfigFactory.parseResources(API_PROTECT_DETECTION_DEFAULT_CONFIGS_PATH);
      return configConverter.convertToAnomalyDetectionConfigs(
          apiProtectConfig.getConfigList(API_PROTECT_DETECTION_CONFIGS_PATH));
    } catch (Exception e) {
      log.error("Error loading API Protect detection configs", e);
      return new ArrayList<>();
    }
  }

  private List<AnomalyDetectionConfig> loadDefaultApiDefinitionDetectionConfigs(
      Config config, List<AnomalyDetectionConfig> apiDefinitionDetectionRegistryConfigs) {
    List<AnomalyDetectionConfig> detectionConfigs =
        configConverter.convertToAnomalyDetectionConfigs(
            config.getConfigList(API_DEFINITION_DETECTION_CONFIGS_PATH));

    Map<String, AnomalyDetectionConfig> configMap =
        detectionConfigs.stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getApiDefinitionMetadataAnomalyDetectionConfig()
                            .getAnomalyRuleId(),
                    anomalyDetectionConfig -> anomalyDetectionConfig));

    return apiDefinitionDetectionRegistryConfigs.stream()
        .map(
            detectionConfig -> {
              String ruleId =
                  detectionConfig
                      .getApiDefinitionMetadataAnomalyDetectionConfig()
                      .getAnomalyRuleId();
              if (configMap.containsKey(ruleId)) {
                return detectionConfig.toBuilder().mergeFrom(configMap.get(ruleId)).build();
              }
              return detectionConfig;
            })
        .collect(Collectors.toList());
  }

  private List<AnomalyDetectionConfig> loadDefaultSessionDefinitionDetectionConfigs(
      Config config, List<AnomalyDetectionConfig> sessionDefinitionDetectionRegistryConfigs) {
    List<AnomalyDetectionConfig> detectionConfigs =
        configConverter.convertToAnomalyDetectionConfigs(
            config.getConfigList(SESSION_DEFINITION_DETECTION_CONFIGS_PATH));

    Map<String, AnomalyDetectionConfig> configMap =
        detectionConfigs.stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getSessionDefinitionMetadataAnomalyDetectionConfig()
                            .getAnomalyRuleId(),
                    anomalyDetectionConfig -> anomalyDetectionConfig));

    return sessionDefinitionDetectionRegistryConfigs.stream()
        .map(
            detectionConfig -> {
              String ruleId =
                  detectionConfig
                      .getSessionDefinitionMetadataAnomalyDetectionConfig()
                      .getAnomalyRuleId();
              if (configMap.containsKey(ruleId)) {
                return detectionConfig.toBuilder().mergeFrom(configMap.get(ruleId)).build();
              }
              return detectionConfig;
            })
        .collect(Collectors.toList());
  }

  private List<AnomalyDetectionConfig> loadDefaultVolumetricDetectionConfigs(
      Config config, List<AnomalyDetectionConfig> volumetricDetectionRegistryConfigs) {
    List<AnomalyDetectionConfig> detectionConfigs =
        configConverter.convertToAnomalyDetectionConfigs(
            config.getConfigList(VOLUMETRIC_DETECTION_CONFIGS_PATH));

    Map<String, AnomalyDetectionConfig> configMap =
        detectionConfigs.stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getVolumetricAnomalyDetectionConfig()
                            .getAnomalyRuleId(),
                    anomalyDetectionConfig -> anomalyDetectionConfig));

    return volumetricDetectionRegistryConfigs.stream()
        .map(
            detectionConfig -> {
              String ruleId =
                  detectionConfig.getVolumetricAnomalyDetectionConfig().getAnomalyRuleId();
              if (configMap.containsKey(ruleId)) {
                return detectionConfig.toBuilder().mergeFrom(configMap.get(ruleId)).build();
              }
              return detectionConfig;
            })
        .collect(Collectors.toList());
  }

  private List<AnomalyDetectionConfig> loadDefaultCredentialStuffingDetectionConfigs(
      Config config, List<AnomalyDetectionConfig> credentialStuffingDetectionRegistryConfigs) {
    List<AnomalyDetectionConfig> detectionConfigs =
        configConverter.convertToAnomalyDetectionConfigs(
            config.getConfigList(CREDENTIAL_STUFFING_DETECTION_CONFIGS_PATH));

    Map<String, AnomalyDetectionConfig> configMap =
        detectionConfigs.stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getCredentialAnomalyDetectionConfig()
                            .getAnomalyRuleId(),
                    anomalyDetectionConfig -> anomalyDetectionConfig));

    return credentialStuffingDetectionRegistryConfigs.stream()
        .map(
            detectionConfig -> {
              String ruleId =
                  detectionConfig.getCredentialAnomalyDetectionConfig().getAnomalyRuleId();
              if (configMap.containsKey(ruleId)) {
                return detectionConfig.toBuilder().mergeFrom(configMap.get(ruleId)).build();
              }
              return detectionConfig;
            })
        .collect(Collectors.toList());
  }

  private List<AnomalyDetectionConfig> loadDefaultAccountTakeoverDetectionConfigs(
      Config config, List<AnomalyDetectionConfig> accountTakeoverDetectionRegistryConfigs) {
    List<AnomalyDetectionConfig> detectionConfigs =
        configConverter.convertToAnomalyDetectionConfigs(
            config.getConfigList(ACCOUNT_TAKEOVER_DETECTION_CONFIGS_PATH));

    Map<String, AnomalyDetectionConfig> configMap =
        detectionConfigs.stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig
                            .getAccountTakeoverAnomalyDetectionConfig()
                            .getAnomalyRuleId(),
                    anomalyDetectionConfig -> anomalyDetectionConfig));

    return accountTakeoverDetectionRegistryConfigs.stream()
        .map(
            detectionConfig -> {
              String ruleId =
                  detectionConfig.getAccountTakeoverAnomalyDetectionConfig().getAnomalyRuleId();
              if (configMap.containsKey(ruleId)) {
                return detectionConfig.toBuilder().mergeFrom(configMap.get(ruleId)).build();
              }
              return detectionConfig;
            })
        .collect(Collectors.toList());
  }

  private List<AnomalyDetectionConfig> loadDefaultGenAiDetectionConfigs(
      Config config, List<AnomalyDetectionConfig> genAiDetectionRegistryConfigs) {
    List<AnomalyDetectionConfig> detectionConfigs =
        configConverter.convertToAnomalyDetectionConfigs(
            config.getConfigList(GEN_AI_DETECTION_CONFIGS_PATH));

    Map<String, AnomalyDetectionConfig> configMap =
        detectionConfigs.stream()
            .collect(
                Collectors.toMap(
                    anomalyDetectionConfig ->
                        anomalyDetectionConfig.getGenAiAnomalyDetectionConfig().getAnomalyRuleId(),
                    anomalyDetectionConfig -> anomalyDetectionConfig));

    return genAiDetectionRegistryConfigs.stream()
        .map(
            detectionConfig -> {
              String ruleId = detectionConfig.getGenAiAnomalyDetectionConfig().getAnomalyRuleId();
              if (configMap.containsKey(ruleId)) {
                return detectionConfig.toBuilder().mergeFrom(configMap.get(ruleId)).build();
              }
              return detectionConfig;
            })
        .collect(Collectors.toList());
  }
}
