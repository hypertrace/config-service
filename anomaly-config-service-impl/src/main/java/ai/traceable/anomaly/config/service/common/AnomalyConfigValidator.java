package ai.traceable.anomaly.config.service.common;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyParamInfo;
import ai.traceable.anomaly.config.service.v1.AnomalyParamInfoScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import io.grpc.Status;
import java.util.regex.Pattern;

public class AnomalyConfigValidator {

  public Status validate(AnomalyConfigScope configScope) {
    AnomalyConfigScope.ScopeCase scopeCase = configScope.getScopeCase();
    switch (scopeCase) {
      case SERVICE_SCOPE:
        return validateServiceScope(configScope.getServiceScope());
      case API_SCOPE:
        return validateApiScope(configScope.getApiScope());
      case PARAM_SCOPE:
        return validateParamScope(configScope.getParamScope());
      default:
        return Status.OK;
    }
  }

  public Status validate(AnomalyConfigScope configScope, boolean checkForCustomerScope) {
    if (checkForCustomerScope) {
      if (configScope.getScopeCase() == AnomalyConfigScope.ScopeCase.SCOPE_NOT_SET) {
        return Status.INVALID_ARGUMENT.withDescription("Anomaly Global Config Scope is not set.");
      }
    }
    return validate(configScope);
  }

  public Status validate(AnomalyConfigStatusChange configStatusChange) {
    if (!configStatusChange.hasDisabled() && !configStatusChange.hasInternal()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config Status Change should have at least one of disabled and internal values");
    }
    return Status.OK;
  }

  private Status validateApiScope(AnomalyApiScope scope) {
    if (scope.getId().isEmpty() || scope.getServiceScope().getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config API Scope should have valid API and Service IDs.");
    }
    return Status.OK;
  }

  private Status validateServiceScope(AnomalyServiceScope scope) {
    if (scope.getId().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config SERVICE Scope should have valid Service ID.");
    }
    return Status.OK;
  }

  private Status validateParamScope(AnomalyParamScope paramScope) {
    Status status;

    if (paramScope.hasScope()) {
      status = validateParamInfoScope(paramScope.getScope());
      if (!status.isOk()) {
        return status;
      }
    } else {
      status = validateApiScope(paramScope.getApiScope());
      if (!status.isOk()) {
        return status;
      }
    }

    if (paramScope.hasParamInfo()) {
      status = validateParamInfo(paramScope.getParamInfo());
      if (!status.isOk()) {
        return status;
      }
    } else if (paramScope.getParamName().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Anomaly Global Config PARAM Scope should have a valid valid param name.");
    }

    return Status.OK;
  }

  private Status validateParamInfo(AnomalyParamInfo paramInfo) {
    switch (paramInfo.getParamDetailsCase()) {
      case PARAM_NAME:
        if (paramInfo.getParamName().isEmpty()) {
          return Status.INVALID_ARGUMENT.withDescription(
              "Param name should not be empty for paramInfo");
        }
        break;
      case PARAM_REGEX:
        if (paramInfo.getParamRegex().isEmpty()) {
          return Status.INVALID_ARGUMENT.withDescription(
              "Param regex should not be empty for paramInfo");
        }
        try {
          Pattern.compile(paramInfo.getParamRegex());
        } catch (Exception e) {
          return Status.INVALID_ARGUMENT.withDescription(
              String.format("Invalid param regex for paramInfo : %s", paramInfo.getParamRegex()));
        }
        break;
      default:
        return Status.INVALID_ARGUMENT.withDescription("ParamInfo is not set");
    }
    return Status.OK;
  }

  private Status validateParamInfoScope(AnomalyParamInfoScope paramInfoScope) {
    switch (paramInfoScope.getScopeCase()) {
      case API_SCOPE:
        return validateApiScope(paramInfoScope.getApiScope());
      case SERVICE_SCOPE:
        return validateServiceScope(paramInfoScope.getServiceScope());
      case CUSTOMER_SCOPE:
        break;
      default:
        return Status.INVALID_ARGUMENT.withDescription("ParamInfoScope is not set");
    }
    return Status.OK;
  }
}
