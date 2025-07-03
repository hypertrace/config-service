package ai.traceable.anomaly.config.service.global.validator;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.RuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersionType;
import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersionsFilter;
import ai.traceable.anomaly.config.service.v1.global.DeleteScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAvailableRuleVersionsRequest;
import ai.traceable.anomaly.config.service.v1.global.GetRulesChangeLogRequest;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetUnresolvedScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.UpdateScopedAnomalyGlobalConfigStatusRequest;
import io.grpc.Status;
import jakarta.inject.Inject;

public class AnomalyGlobalConfigServiceValidator implements GlobalConfigValidator {
  private final AnomalyConfigValidator anomalyConfigValidator;

  @Inject
  public AnomalyGlobalConfigServiceValidator(AnomalyConfigValidator anomalyConfigValidator) {
    this.anomalyConfigValidator = anomalyConfigValidator;
  }

  @Override
  public Status validate(GetScopedAnomalyGlobalConfigStatusRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Update request should have a valid config scope.");
    }
    return anomalyConfigValidator.validate(request.getConfigScope());
  }

  @Override
  public Status validate(GetUnresolvedScopedAnomalyGlobalConfigStatusRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Update request should have a valid config scope.");
    }
    return anomalyConfigValidator.validate(request.getConfigScope());
  }

  @Override
  public Status validate(UpdateScopedAnomalyGlobalConfigStatusRequest request) {
    if (!request.hasScopedConfig()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Update request should have a valid scoped config change object.");
    }
    if (!request.getScopedConfig().hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Update request should have a valid config scope.");
    }
    Status status = anomalyConfigValidator.validate(request.getScopedConfig().getConfigScope());
    if (status != Status.OK) {
      return status;
    }
    if (request.getScopedConfig().hasConfigStatus()) {
      return anomalyConfigValidator.validate(request.getScopedConfig().getConfigStatus());
    }
    return status;
  }

  @Override
  public Status validate(GetAnomalyRuleInfosRequest request) {
    for (AnomalyEventFamily eventFamily : request.getEventFamiliesList()) {
      if (eventFamily == AnomalyEventFamily.ANOMALY_EVENT_FAMILY_UNSPECIFIED) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format("Event family type: %s not defined", eventFamily));
      }
    }
    return Status.OK;
  }

  @Override
  public Status validate(DeleteScopedAnomalyGlobalConfigStatusRequest request) {
    if (!request.hasConfigScope()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Delete request should have a valid config scope.");
    }
    return anomalyConfigValidator.validate(request.getConfigScope());
  }

  @Override
  public Status validate(GetAvailableRuleVersionsRequest request) {
    if (request.getRuleType().equals(RuleType.RULE_TYPE_UNSPECIFIED)) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format("Rule type: %s not defined", request.getRuleType()));
    }
    if (request.hasFilter()) {
      AvailableRuleVersionsFilter filter = request.getFilter();
      for (RuleVersionType versionType : filter.getVersionTypesList()) {
        if (versionType.equals(RuleVersionType.RULE_VERSION_TYPE_UNSPECIFIED)) {
          return Status.INVALID_ARGUMENT.withDescription(
              "Version type must not be RULE_VERSION_TYPE_UNSPECIFIED");
        }
        if (request.getRuleType().equals(RuleType.RULE_TYPE_API_PROTECTION)
            && versionType.equals(RuleVersionType.RULE_VERSION_TYPE_EXPERIMENTAL)) {
          return Status.INVALID_ARGUMENT.withDescription(
              "Rule version type must not be RULE_VERSION_TYPE_EXPERIMENTAL for API protection");
        }
      }
    }
    return Status.OK;
  }

  public Status validate(GetRulesChangeLogRequest request) {
    if (request.getRuleType().equals(RuleType.RULE_TYPE_UNSPECIFIED)) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format("Rule type: %s not defined", request.getRuleType()));
    }
    if (!request.hasCurrentVersion() || !request.hasPreviousVersion()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Both current and previous rule versions must be specified");
    }
    return Status.OK;
  }
}
