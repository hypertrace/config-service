package ai.traceable.anomaly.config.service.override.exclusion;

import ai.traceable.anomaly.config.service.override.common.validator.DetectionOverrideConfigsValidator;
import ai.traceable.anomaly.config.service.override.common.validator.DetectionOverrideEventsValidator;
import ai.traceable.anomaly.config.service.override.common.validator.DetectionOverrideScopeValidator;
import ai.traceable.anomaly.config.service.v1.override.CreateDetectionExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.override.DeleteDetectionExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.override.DetectionExclusionConfigCriteria;
import ai.traceable.anomaly.config.service.v1.override.DetectionExclusionRule;
import ai.traceable.anomaly.config.service.v1.override.DetectionExclusionRuleInfo;
import ai.traceable.anomaly.config.service.v1.override.UpdateDetectionExclusionRuleRequest;
import io.grpc.Status;
import javax.inject.Inject;
import lombok.NonNull;

public class DetectionOverrideExclusionRulesValidator implements ExclusionRulesValidator {

  private final DetectionOverrideConfigsValidator configsValidator;
  private final DetectionOverrideEventsValidator eventsValidator;
  private final DetectionOverrideScopeValidator scopeValidator;

  @Inject
  public DetectionOverrideExclusionRulesValidator(
      DetectionOverrideConfigsValidator configsValidator,
      DetectionOverrideEventsValidator eventsValidator,
      DetectionOverrideScopeValidator scopeValidator) {
    this.configsValidator = configsValidator;
    this.eventsValidator = eventsValidator;
    this.scopeValidator = scopeValidator;
  }

  @Override
  public Status validate(CreateDetectionExclusionRuleRequest request) {
    if (!request.hasRuleInfo() || !request.hasRuleScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Create detection exclusion rule request should have a valid rule info / rule scope.");
    }

    Status status;
    if ((status = validateRuleInfo(request.getRuleInfo())) != Status.OK) {
      return status;
    }
    if ((status = scopeValidator.validateRuleScope(request.getRuleScope())) != Status.OK) {
      return status;
    }

    return Status.OK;
  }

  @Override
  public Status validate(UpdateDetectionExclusionRuleRequest request) {
    if (!request.hasRule()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Update detection exclusion rule request should have a valid rule.");
    }
    return validateDetectionExclusionRule(request.getRule());
  }

  @Override
  public Status validate(DeleteDetectionExclusionRuleRequest request) {
    if (request.getRuleId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Delete Detection Exclusion rule request shouldn't have an empty id");
    }
    return Status.OK;
  }

  private Status validateDetectionExclusionRule(@NonNull DetectionExclusionRule rule) {
    if (rule.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Detection rule shouldn't have an empty id.");
    }

    if (!rule.hasRuleScope() || !rule.hasRuleInfo() || !rule.hasRuleStatus()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Detection rule should have a valid scope/info/status.");
    }

    Status status;
    if ((status = scopeValidator.validateRuleScope(rule.getRuleScope())) != Status.OK) {
      return status;
    }
    if ((status = validateRuleInfo(rule.getRuleInfo())) != Status.OK) {
      return status;
    }

    return Status.OK;
  }

  private Status validateRuleInfo(@NonNull DetectionExclusionRuleInfo ruleInfo) {
    if (ruleInfo.getName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("Rule info shouldn't have an empty name.");
    }

    if (!ruleInfo.hasCriteria()) {
      return Status.INVALID_ARGUMENT.withDescription("Rule info should have a valid criteria.");
    }
    return validateCriteria(ruleInfo.getCriteria());
  }

  private Status validateCriteria(@NonNull DetectionExclusionConfigCriteria criteria) {
    if (!criteria.hasTargetEventsConfig()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Criteria should have a valid target events config.");
    }

    Status status;
    if ((status = eventsValidator.validateTargetEventsConfig(criteria.getTargetEventsConfig()))
        != Status.OK) {
      return status;
    }
    if (criteria.hasTargetAssetConfig()
        && (status = configsValidator.validateAssetConfig(criteria.getTargetAssetConfig()))
            != Status.OK) {
      return status;
    }
    if (criteria.hasSourceAssetConfig()
        && (status = configsValidator.validateAssetConfig(criteria.getSourceAssetConfig()))
            != Status.OK) {
      return status;
    }
    if (criteria.hasActorsConfig()
        && (status = configsValidator.validateActorsConfig(criteria.getActorsConfig()))
            != Status.OK) {
      return status;
    }
    return Status.OK;
  }
}
