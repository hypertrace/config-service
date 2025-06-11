package ai.traceable.anomaly.config.service.detector.anomalydetection;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiStateBasedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.BlockingMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomRulesAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomRulesAnomalyDetectionConfig.EmailDomainAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.CustomRulesAnomalyDetectionConfig.MaliciousSourcesRulesAnomalyConfig;
import ai.traceable.anomaly.config.service.v1.detector.DeleteAnomalyConfigOption;
import ai.traceable.anomaly.config.service.v1.detector.DeleteScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetGlobalResolvedScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetUnresolvedScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UpdateScopedAnomalyDetectionConfigRequest;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class AnomalyDetectionConfigValidator {
  private final AnomalyConfigValidator anomalyConfigValidator;
  private final ModsecConfigValidator modsecConfigValidator;
  private final AnomalyDetectionConfigRegexValidator anomalyDetectionConfigRegexValidator;
  private final Map<String, ApiDefinitionMetadataAnomalyDetectionConfig> apiDefRuleIdToConfigMap;
  private final Map<String, SessionDefinitionMetadataAnomalyDetectionConfig>
      sessionDefRuleIdToConfigMap;

  @Inject
  public AnomalyDetectionConfigValidator(
      AnomalyConfigValidator anomalyConfigValidator,
      ModsecConfigValidator modsecConfigValidator,
      AnomalyDetectionConfigRegexValidator anomalyDetectionConfigRegexValidator,
      ApiDefinitionRegistry apiDefinitionRegistry,
      SessionRulesRegistry sessionRulesRegistry,
      ModsecRulesRegistry modsecRulesRegistry) {
    this.anomalyConfigValidator = anomalyConfigValidator;
    this.anomalyDetectionConfigRegexValidator = anomalyDetectionConfigRegexValidator;
    this.apiDefRuleIdToConfigMap = apiDefinitionRegistry.getApiDefRuleIdToDetectionConfigMap();
    this.sessionDefRuleIdToConfigMap =
        sessionRulesRegistry.getSessionDefRuleIdToDetectionConfigMap();
    this.modsecConfigValidator = modsecConfigValidator;
  }

  public Status validate(GetScopedAnomalyDetectionConfigRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "GetScopedAnomalyDetectionConfigRequest should have a valid config scope.");
    }
    return anomalyConfigValidator.validate(request.getConfigScope(), true);
  }

  public Status validate(GetGlobalResolvedScopedAnomalyDetectionConfigRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "GetGlobalResolvedScopedAnomalyDetectionConfigRequest should have a valid config scope.");
    }
    return anomalyConfigValidator.validate(request.getConfigScope(), true);
  }

  public Status validate(GetUnresolvedScopedAnomalyDetectionConfigRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "GetUnresolvedScopedAnomalyDetectionConfigRequest should have a valid config scope.");
    }
    Status status = anomalyConfigValidator.validate(request.getConfigScope(), true);
    if (!status.isOk()) {
      return status;
    }
    if (!request.hasFilter()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "GetUnresolvedScopedAnomalyDetectionConfigRequest should have a valid config filter.");
    }

    return Status.OK;
  }

  public Status validate(DeleteScopedAnomalyDetectionConfigRequest request) {
    if (!request.getScopedAnomalyDetectionConfig().hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "DeleteScopedAnomalyDetectionConfigRequest should have a valid config scope.");
    }
    if (request
        .getDeleteAnomalyConfigOption()
        .equals(DeleteAnomalyConfigOption.DELETE_ANOMALY_CONFIG_OPTION_UNSPECIFIED)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "DeleteScopedAnomalyDetectionConfigRequest should have a valid delete option.");
    }
    return anomalyConfigValidator.validate(
        request.getScopedAnomalyDetectionConfig().getConfigScope(), true);
  }

  public Status validate(
      UpdateScopedAnomalyDetectionConfigRequest request, RequestContext requestContext) {
    if (!request.getScopedAnomalyDetectionConfig().hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "UpdateScopedAnomalyDetectionConfigRequest should have a valid config scope.");
    }
    Status status =
        validate(
            request.getScopedAnomalyDetectionConfig().getAnomalyDetectionConfigsList(),
            request.getScopedAnomalyDetectionConfig().getConfigScope(),
            requestContext);
    if (status == Status.OK) {
      return anomalyConfigValidator.validate(
          request.getScopedAnomalyDetectionConfig().getConfigScope(), true);
    }
    return status;
  }

  private Status validate(
      List<AnomalyDetectionConfig> detectionConfigs,
      AnomalyConfigScope configScope,
      RequestContext requestContext) {
    List<ModsecurityAnomalyDetectionConfig> modsecRuleConfigs =
        detectionConfigs.stream()
            .filter(AnomalyDetectionConfig::hasModsecurityAnomalyDetectionConfig)
            .map(AnomalyDetectionConfig::getModsecurityAnomalyDetectionConfig)
            .collect(Collectors.toList());

    Status status = modsecConfigValidator.validate(modsecRuleConfigs, requestContext, configScope);

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

    if (!status.isOk()) {
      return status;
    }

    List<CustomRulesAnomalyDetectionConfig> customRulesAnomalyDetectionConfigs =
        detectionConfigs.stream()
            .filter(AnomalyDetectionConfig::hasCustomRulesAnomalyDetectionConfig)
            .map(AnomalyDetectionConfig::getCustomRulesAnomalyDetectionConfig)
            .collect(Collectors.toList());

    status = validateCustomRulesDetectionConfigs(customRulesAnomalyDetectionConfigs);

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
      Status status =
          anomalyDetectionConfigRegexValidator
              .validateSessionDefinitionMetadataAnomalyDetectionConfigRegex(detectionConfig);
      if (!status.isOk()) {
        return status;
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
      Status status =
          anomalyDetectionConfigRegexValidator
              .validateApiDefinitionMetadataAnomalyDetectionConfigRegex(detectionConfig);
      if (!status.isOk()) {
        return status;
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
   * @param stateBasedDetectionConfigs
   * @return Status.INVALID_ARGUMENT in case of presence of configs with same config case. Status.OK
   *     in all other cases.
   */
  private Status validateStateBasedDetectionConfigs(
      List<ApiStateBasedAnomalyDetectionConfig> stateBasedDetectionConfigs) {
    try {
      Status status =
          stateBasedDetectionConfigs.stream()
              .map(
                  anomalyDetectionConfigRegexValidator
                      ::validateApiStateBasedAnomalyDetectionConfigRegex)
              .filter(Predicate.not(Status::isOk))
              .findFirst()
              .orElse(Status.OK);
      if (!status.isOk()) {
        return status;
      }
      stateBasedDetectionConfigs.stream()
          .collect(
              Collectors.toMap(
                  ApiStateBasedAnomalyDetectionConfig::getConfigCase, Function.identity()));
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
                  BlockingMetadataAnomalyDetectionConfig::getConfigCase, Function.identity()));
    } catch (IllegalStateException e) {
      return Status.INVALID_ARGUMENT.withDescription(e.getMessage());
    }
    return Status.OK;
  }

  /**
   * @param customRulesAnomalyDetectionConfigs
   * @return Status.INVALID_ARGUMENT in case of presence of configs with same config case or
   *     presence of emailDomainConfigs with highThreshold > criticalThreshold . Status.OK in all
   *     other cases.
   */
  private Status validateCustomRulesDetectionConfigs(
      List<CustomRulesAnomalyDetectionConfig> customRulesAnomalyDetectionConfigs) {
    try {
      customRulesAnomalyDetectionConfigs.stream()
          .collect(
              Collectors.toMap(
                  CustomRulesAnomalyDetectionConfig::getConfigCase, Function.identity()));
    } catch (IllegalStateException e) {
      return Status.INVALID_ARGUMENT.withDescription(e.getMessage());
    }

    List<EmailDomainAnomalyConfig> emailDomainAnomalyConfigs =
        customRulesAnomalyDetectionConfigs.stream()
            .map(CustomRulesAnomalyDetectionConfig::getMaliciousSources)
            .map(MaliciousSourcesRulesAnomalyConfig::getEmailDomain)
            .collect(Collectors.toList());

    return validateEmailDomainConfigs(emailDomainAnomalyConfigs);
  }

  private Status validateEmailDomainConfigs(
      List<EmailDomainAnomalyConfig> emailDomainAnomalyConfigs) {
    boolean hasInvalidConfig =
        emailDomainAnomalyConfigs.stream()
            .anyMatch(
                config ->
                    config.getHighEmailFraudScoreMinThreshold()
                        > config.getCriticalEmailFraudScoreMinThreshold());

    return hasInvalidConfig
        ? Status.INVALID_ARGUMENT.withDescription(
            "Invalid EmailDomainAnomalyConfig: highEmailFraudScoreMinThreshold > criticalEmailFraudScoreMinThreshold")
        : Status.OK;
  }
}
