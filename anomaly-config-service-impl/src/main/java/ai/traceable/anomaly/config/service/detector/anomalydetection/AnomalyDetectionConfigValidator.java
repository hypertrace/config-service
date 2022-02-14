package ai.traceable.anomaly.config.service.detector.anomalydetection;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleInfo;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiStateBasedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.BlockingMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AnomalyDetectionConfigValidator {
  private final AnomalyConfigValidator anomalyConfigValidator;
  private final Map<String, ApiDefinitionMetadataAnomalyDetectionConfig> apiDefRuleIdToConfigMap;
  private final Map<String, SessionDefinitionMetadataAnomalyDetectionConfig>
      sessionDefRuleIdToConfigMap;
  private final Map<String, AnomalyRuleInfo> modsecRuleInfoMap;

  @Inject
  public AnomalyDetectionConfigValidator(
      AnomalyConfigValidator anomalyConfigValidator,
      ApiDefinitionRegistry apiDefinitionRegistry,
      SessionRulesRegistry sessionRulesRegistry,
      ModsecRulesRegistry modsecRulesRegistry) {
    this.anomalyConfigValidator = anomalyConfigValidator;
    this.apiDefRuleIdToConfigMap = apiDefinitionRegistry.getApiDefRuleIdToDetectionConfigMap();
    this.sessionDefRuleIdToConfigMap =
        sessionRulesRegistry.getSessionDefRuleIdToDetectionConfigMap();
    this.modsecRuleInfoMap = modsecRulesRegistry.getModsecRuleInfos();
  }

  public Status validate(GetScopedAnomalyDetectionConfigRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "GetScopedAnomalyDetectionConfigRequest should have a valid config scope.");
    }
    return anomalyConfigValidator.validate(request.getConfigScope(), true);
  }

  public Status validate(UpdateScopedAnomalyDetectionConfigRequest request) {
    if (!request.getScopedAnomalyDetectionConfig().hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "UpdateScopedAnomalyDetectionConfigRequest should have a valid config scope.");
    }
    Status status =
        validate(request.getScopedAnomalyDetectionConfig().getAnomalyDetectionConfigsList());
    if (status == Status.OK) {
      return anomalyConfigValidator.validate(
          request.getScopedAnomalyDetectionConfig().getConfigScope(), true);
    }
    return status;
  }

  private Status validate(List<AnomalyDetectionConfig> detectionConfigs) {
    List<ModsecurityAnomalyDetectionConfig> modsecRuleConfigs =
        detectionConfigs.stream()
            .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
            .map(AnomalyDetectionConfig::getModsecurityAnomalyDetectionConfig)
            .collect(Collectors.toList());

    Status status = validateModsecConfigs(modsecRuleConfigs);

    if (!status.isOk()) {
      return status;
    }

    List<ApiDefinitionMetadataAnomalyDetectionConfig> apiDefinitionDetectionConfigs =
        detectionConfigs.stream()
            .filter(AnomalyDetectionConfig::hasApiDefinitionMetadataAnomalyDetectionConfig)
            .map(AnomalyDetectionConfig::getApiDefinitionMetadataAnomalyDetectionConfig)
            .collect(Collectors.toList());

    status = validateApiDefinitionConfigs(apiDefinitionDetectionConfigs);

    if (!status.isOk()) {
      return status;
    }

    List<ApiStateBasedAnomalyDetectionConfig> stateBasedDetectionConfigs =
        detectionConfigs.stream()
            .filter(AnomalyDetectionConfig::hasApiStateBasedAnomalyDetectionConfig)
            .map(AnomalyDetectionConfig::getApiStateBasedAnomalyDetectionConfig)
            .collect(Collectors.toList());

    status = validateStateBasedDetectionConfigs(stateBasedDetectionConfigs);

    if (!status.isOk()) {
      return status;
    }

    List<SessionDefinitionMetadataAnomalyDetectionConfig>
        sessionDefinitionMetadataDetectionConfigs =
            detectionConfigs.stream()
                .filter(AnomalyDetectionConfig::hasSessionDefinitionMetadataAnomalyDetectionConfig)
                .map(AnomalyDetectionConfig::getSessionDefinitionMetadataAnomalyDetectionConfig)
                .collect(Collectors.toList());

    status = validateSessionDefinitionMetadataConfigs(sessionDefinitionMetadataDetectionConfigs);

    if (!status.isOk()) {
      return status;
    }

    List<BlockingMetadataAnomalyDetectionConfig> blockingAnomalyDetectionConfigs =
        detectionConfigs.stream()
            .filter(AnomalyDetectionConfig::hasBlockingMetadataAnomalyDetectionConfig)
            .map(AnomalyDetectionConfig::getBlockingMetadataAnomalyDetectionConfig)
            .collect(Collectors.toList());

    status = validateBlockingDetectionConfigs(blockingAnomalyDetectionConfigs);

    return status;
  }

  private Status validateSessionDefinitionMetadataConfigs(
      List<SessionDefinitionMetadataAnomalyDetectionConfig> sessionDefinitionMetadataConfigs) {
    Map<String, SessionDefinitionMetadataAnomalyDetectionConfig> ruleIdMap = new HashMap<>();
    EnumMap<
            SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase,
            SessionDefinitionMetadataAnomalyDetectionConfig>
        configCaseMap =
            new EnumMap<>(SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase.class);

    for (SessionDefinitionMetadataAnomalyDetectionConfig detectionConfig :
        sessionDefinitionMetadataConfigs) {
      String ruleId = detectionConfig.getAnomalyRuleId();
      SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase =
          detectionConfig.getConfigCase();
      if (ruleId.isEmpty()) {
        if (configCase.equals(
            SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase.CONFIG_NOT_SET)) {
          return Status.INVALID_ARGUMENT.withDescription(
              "Invalid session definition metadata detection config");
        }
      } else {
        if (!sessionDefRuleIdToConfigMap.containsKey(ruleId)) {
          return Status.INVALID_ARGUMENT.withDescription(
              String.format(
                  "Invalid session definition metadata detection config ruleId: %s", ruleId));
        }
        if (!configCase.equals(
                SessionDefinitionMetadataAnomalyDetectionConfig.ConfigCase.CONFIG_NOT_SET)
            && !configCase.equals(sessionDefRuleIdToConfigMap.get(ruleId).getConfigCase())) {
          return Status.INVALID_ARGUMENT.withDescription(
              String.format(
                  "Invalid session definition metadata detection config type for ruleId: %s",
                  ruleId));
        }
      }

      if (ruleIdMap.containsKey(ruleId)) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "UpdateScopedAnomalyDetectionConfigRequest should have only one session definition metadata detection config for ruleId: %s",
                ruleId));
      }

      if (configCaseMap.containsKey(configCase)) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "UpdateScopedAnomalyDetectionConfigRequest should have only one session definition metadata detection config for configCase: %s",
                configCase));
      }

      if (ruleId.isEmpty()) {
        configCaseMap.put(configCase, detectionConfig);
      } else {
        ruleIdMap.put(ruleId, detectionConfig);
      }
    }
    return Status.OK;
  }
  /**
   * @param apiDefinitionDetectionConfigs
   * @return Status.INVALID_ARGUMENT in case both ruleId and config case are not present, ruleId is
   *     not present in apiDef registry, mismatch of ruleId and config case or presence of configs
   *     with same ruleId or configCase. Status.OK in all other cases.
   */
  private Status validateApiDefinitionConfigs(
      List<ApiDefinitionMetadataAnomalyDetectionConfig> apiDefinitionDetectionConfigs) {

    Map<String, ApiDefinitionMetadataAnomalyDetectionConfig> ruleIdMap = new HashMap<>();
    EnumMap<
            ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase,
            ApiDefinitionMetadataAnomalyDetectionConfig>
        configCaseMap = new EnumMap<>(ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.class);

    for (ApiDefinitionMetadataAnomalyDetectionConfig detectionConfig :
        apiDefinitionDetectionConfigs) {
      String ruleId = detectionConfig.getAnomalyRuleId();
      ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase configCase =
          detectionConfig.getConfigCase();

      if (ruleId.isEmpty()) {
        if (configCase.equals(
            ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.CONFIG_NOT_SET)) {
          return Status.INVALID_ARGUMENT.withDescription("Invalid api definition detection config");
        }
      } else {
        if (!apiDefRuleIdToConfigMap.containsKey(ruleId)) {
          return Status.INVALID_ARGUMENT.withDescription(
              String.format("Invalid api definition detection config ruleId: %s", ruleId));
        }
        if (!configCase.equals(
                ApiDefinitionMetadataAnomalyDetectionConfig.ConfigCase.CONFIG_NOT_SET)
            && !configCase.equals(apiDefRuleIdToConfigMap.get(ruleId).getConfigCase())) {
          return Status.INVALID_ARGUMENT.withDescription(
              String.format("Invalid api definition detection config type for ruleId: %s", ruleId));
        }
      }

      if (ruleIdMap.containsKey(ruleId)) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "UpdateScopedAnomalyDetectionConfigRequest should have only one api definition detection config for ruleId: %s",
                ruleId));
      }

      if (configCaseMap.containsKey(configCase)) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "UpdateScopedAnomalyDetectionConfigRequest should have only one api definition detection config for configCase: %s",
                configCase));
      }

      if (ruleId.isEmpty()) {
        configCaseMap.put(configCase, detectionConfig);
      } else {
        ruleIdMap.put(ruleId, detectionConfig);
      }
    }

    return Status.OK;
  }

  /**
   * @param modsecConfigs
   * @return Status.INVALID_ARGUMENT in case of presence of configs with same ruleId or presence of
   *     subRuleConfigs with same subRuleId for a particular modsec config. Status.OK in all other
   *     cases.
   */
  private Status validateModsecConfigs(List<ModsecurityAnomalyDetectionConfig> modsecConfigs) {
    Set<String> ruleIds = new HashSet<>();
    ModsecurityAnomalyDetectionConfig modsecAllDetectionConfig =
        ModsecurityAnomalyDetectionConfig.getDefaultInstance();
    for (ModsecurityAnomalyDetectionConfig detectionConfig : modsecConfigs) {
      if (detectionConfig.hasModsecAnomalyRule()) {
        ModsecurityAnomalyRuleConfig modsecRuleConfig = detectionConfig.getModsecAnomalyRule();
        String ruleId = modsecRuleConfig.getAnomalyRuleId();

        if (ruleIds.contains(ruleId)) {
          return Status.INVALID_ARGUMENT.withDescription(
              String.format(
                  "UpdateScopedAnomalyDetectionConfigRequest should have only one modsec config with ruleId: %s",
                  ruleId));
        } else if (!modsecRuleInfoMap.containsKey(ruleId)) {
          return Status.INVALID_ARGUMENT.withDescription(
              String.format("Invalid modsec ruleId: %s", ruleId));
        } else {
          AnomalyRuleInfo ruleInfo = modsecRuleInfoMap.get(ruleId);
          Status status =
              validateSubRuleConfigs(modsecRuleConfig.getSubRuleConfigsList(), ruleInfo, ruleId);
          if (!status.isOk()) {
            return status;
          }
          ruleIds.add(ruleId);
        }
      } else if (detectionConfig.hasModsecAllDetection()) {
        if (modsecAllDetectionConfig.equals(
            ModsecurityAnomalyDetectionConfig.getDefaultInstance())) {
          modsecAllDetectionConfig = detectionConfig;
        } else {
          return Status.INVALID_ARGUMENT.withDescription(
              "UpdateScopedAnomalyDetectionConfigRequest should have only one modsecAllDetectionConfig");
        }
      }
    }
    return Status.OK;
  }

  /**
   * @param stateBasedDetectionConfigs
   * @return Status.INVALID_ARGUMENT in case of presence of configs with same config case. Status.OK
   *     in all other cases.
   */
  private Status validateStateBasedDetectionConfigs(
      List<ApiStateBasedAnomalyDetectionConfig> stateBasedDetectionConfigs) {
    try {
      stateBasedDetectionConfigs.stream()
          .collect(
              Collectors.toMap(
                  ApiStateBasedAnomalyDetectionConfig::getConfigCase,
                  detectionConfig -> detectionConfig));
    } catch (IllegalStateException e) {
      return Status.INVALID_ARGUMENT.withDescription(e.getMessage());
    }
    return Status.OK;
  }

  /**
   * @param blockingAnomalyDetectionConfigs
   * @return Status.INVALID_ARGUMENT in case of presence of configs with same config case. Status.OK
   *     in all other cases.
   */
  private Status validateBlockingDetectionConfigs(
      List<BlockingMetadataAnomalyDetectionConfig> blockingAnomalyDetectionConfigs) {
    try {
      blockingAnomalyDetectionConfigs.stream()
          .collect(
              Collectors.toMap(
                  BlockingMetadataAnomalyDetectionConfig::getConfigCase,
                  detectionConfig -> detectionConfig));
    } catch (IllegalStateException e) {
      return Status.INVALID_ARGUMENT.withDescription(e.getMessage());
    }
    return Status.OK;
  }

  private Status validateSubRuleConfigs(
      List<AnomalySubRuleConfig> subRuleConfigs, AnomalyRuleInfo ruleInfo, String ruleId) {
    Set<String> subRuleIds = new HashSet<>();
    Map<String, List<AnomalySubRuleType>> subRuleInfoMap = getSubRuleInfoMap(ruleInfo);

    for (AnomalySubRuleConfig subRuleConfig : subRuleConfigs) {
      String subRuleId = subRuleConfig.getSubRuleId();

      if (!subRuleInfoMap.containsKey(subRuleId)) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format("Invalid subRuleId: %s for modsec ruleId: %s", subRuleId, ruleId));
      }

      if (subRuleConfig.hasBlockingEnabled()) {
        List<AnomalySubRuleType> subRuleTypes = subRuleInfoMap.get(subRuleId);
        if (!subRuleTypes.contains(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)) {
          return Status.INVALID_ARGUMENT.withDescription(
              String.format(
                  "SubRuleId: %s not available for blocking for modsec ruleId: %s",
                  subRuleId, ruleId));
        }
      }

      if (subRuleIds.contains(subRuleId)) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format(
                "UpdateScopedAnomalyDetectionConfigRequest should have only one subRule config with subRuleId: %s in modsec config with ruleId: %s",
                subRuleId, ruleId));
      } else {
        subRuleIds.add(subRuleId);
      }
    }

    return Status.OK;
  }

  private Map<String, List<AnomalySubRuleType>> getSubRuleInfoMap(AnomalyRuleInfo ruleInfo) {
    Map<String, List<AnomalySubRuleType>> subRuleInfoMap = new HashMap<>();

    for (AnomalySubRuleInfo subRuleInfo : ruleInfo.getSubRuleInfosList()) {
      subRuleInfoMap.put(subRuleInfo.getRuleId(), subRuleInfo.getSubRuleTypesList());
    }

    return subRuleInfoMap;
  }
}
