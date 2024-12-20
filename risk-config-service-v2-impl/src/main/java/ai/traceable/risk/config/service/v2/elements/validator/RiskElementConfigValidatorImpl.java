package ai.traceable.risk.config.service.v2.elements.validator;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;

import ai.traceable.risk.config.service.v2.AuthCategoryPredicate;
import ai.traceable.risk.config.service.v2.AuthCategoryValue;
import ai.traceable.risk.config.service.v2.EnumOperator;
import ai.traceable.risk.config.service.v2.IntOperator;
import ai.traceable.risk.config.service.v2.IntPredicate;
import ai.traceable.risk.config.service.v2.PathParamTypePredicate;
import ai.traceable.risk.config.service.v2.PathParamTypeValue;
import ai.traceable.risk.config.service.v2.ResponseSensitivityPredicate;
import ai.traceable.risk.config.service.v2.ResponseSensitivityValue;
import ai.traceable.risk.config.service.v2.RiskConfigServiceRequestValidator;
import ai.traceable.risk.config.service.v2.RiskElementConfig;
import ai.traceable.risk.config.service.v2.RiskElementConfigUpdateDetails;
import ai.traceable.risk.config.service.v2.RiskElementConfigUpdates;
import ai.traceable.risk.config.service.v2.RiskElementPredicate;
import ai.traceable.risk.config.service.v2.StringOperator;
import ai.traceable.risk.config.service.v2.StringPredicate;
import ai.traceable.risk.config.service.v2.VulnerabilitySeverityPredicate;
import ai.traceable.risk.config.service.v2.VulnerabilitySeverityValue;
import io.grpc.Status;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class RiskElementConfigValidatorImpl implements RiskElementConfigValidator {

  private final RiskConfigServiceRequestValidator requestValidator;

  @Override
  public void validateRiskElementUpdateDetails(RiskElementConfigUpdates riskElementConfigUpdates) {
    validateNonDefaultPresenceOrThrow(
        riskElementConfigUpdates,
        RiskElementConfigUpdates.RISK_ELEMENT_CONFIG_UPDATE_DETAILS_FIELD_NUMBER);
    for (RiskElementConfigUpdateDetails updateDetails :
        riskElementConfigUpdates.getRiskElementConfigUpdateDetailsList()) {
      validateNonDefaultPresenceOrThrow(
          updateDetails, RiskElementConfigUpdateDetails.ID_FIELD_NUMBER);
      Status status = requestValidator.validateElementId(updateDetails.getId());
      if (!status.isOk()) {
        throw status.asRuntimeException();
      }

      if (!updateDetails.hasRiskElementScoring()) {
        throw Status.NOT_FOUND
            .withDescription("No element config update score provided in request")
            .asRuntimeException();
      }

      status =
          requestValidator.validateScoreValue(updateDetails.getRiskElementScoring().getScore());
      if (!status.isOk()) {
        throw status.asRuntimeException();
      }
    }
  }

  @Override
  public Status validateRiskElementConfig(RiskElementConfig riskElementConfig) {
    Status status = requestValidator.validateElementId(riskElementConfig.getId());
    if (!status.isOk()) {
      return status;
    }
    if (!riskElementConfig.hasRiskElementScoring()) {
      return Status.NOT_FOUND.withDescription("No element config score provided");
    }

    status =
        requestValidator.validateScoreValue(riskElementConfig.getRiskElementScoring().getScore());
    if (!status.isOk()) {
      return status;
    }
    if (!riskElementConfig.hasRiskElementPredicate()) {
      return Status.NOT_FOUND.withDescription("No element predicate present");
    }
    return validateRiskElementPredicate(riskElementConfig.getRiskElementPredicate());
  }

  private Status validateRiskElementPredicate(RiskElementPredicate riskElementPredicate) {
    switch (riskElementPredicate.getTypeCase()) {
      case TYPE_NOT_SET:
        return Status.NOT_FOUND.withDescription("No predicate type found");
      case LABEL_ID:
        return validateStringPredicate(riskElementPredicate.getLabelId());
      case DEPENDENT_APIS:
        return validateIntegerPredicate(riskElementPredicate.getDependentApis());
      case THIRD_PARTY_APIS:
        return validateIntegerPredicate(riskElementPredicate.getThirdPartyApis());
      case PATH_PARAM_TYPE:
        return validatePathParamTypePredicate(riskElementPredicate.getPathParamType());
      case VULNERABILITY_SEVERITY:
        return validateVulnerabilitySeverityPredicate(
            riskElementPredicate.getVulnerabilitySeverity());
      case EXTERNAL_ENCRYPTED_AUTH_CATEGORY:
        return validateAuthCategoryPredicate(
            riskElementPredicate.getExternalEncryptedAuthCategory());
      case EXTERNAL_UNENCRYPTED_AUTH_CATEGORY:
        return validateAuthCategoryPredicate(
            riskElementPredicate.getExternalUnencryptedAuthCategory());
      case INTERNAL_ENCRYPTED_AUTH_CATEGORY:
        return validateAuthCategoryPredicate(
            riskElementPredicate.getInternalEncryptedAuthCategory());
      case INTERNAL_UNENCRYPTED_AUTH_CATEGORY:
        return validateAuthCategoryPredicate(
            riskElementPredicate.getInternalUnencryptedAuthCategory());
      case RESPONSE_SENSITIVITY:
        return validateResponseSensitivityPredicate(riskElementPredicate.getResponseSensitivity());
      default:
        return Status.OK;
    }
  }

  private Status validateResponseSensitivityPredicate(ResponseSensitivityPredicate enumPredicate) {
    if (!isValidEnumOperator(enumPredicate.getOperator())) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid Enum Operator");
    }
    if (!isValidResponseSensitivityValue(enumPredicate.getValue())) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid Response Sensitivity Value");
    }
    return Status.OK;
  }

  private boolean isValidResponseSensitivityValue(ResponseSensitivityValue value) {
    return !(value.equals(ResponseSensitivityValue.RESPONSE_SENSITIVITY_VALUE_UNSPECIFIED)
        || value.equals(ResponseSensitivityValue.UNRECOGNIZED));
  }

  private Status validateAuthCategoryPredicate(AuthCategoryPredicate enumPredicate) {
    if (!isValidEnumOperator(enumPredicate.getOperator())) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid Enum Operator");
    }
    if (!isValidAuthCategoryValue(enumPredicate.getValue())) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid Auth Category Value");
    }
    return Status.OK;
  }

  private boolean isValidAuthCategoryValue(AuthCategoryValue value) {
    return !(value.equals(AuthCategoryValue.AUTH_CATEGORY_VALUE_UNSPECIFIED)
        || value.equals(AuthCategoryValue.UNRECOGNIZED));
  }

  private Status validateVulnerabilitySeverityPredicate(
      VulnerabilitySeverityPredicate enumPredicate) {
    if (!isValidEnumOperator(enumPredicate.getOperator())) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid Enum Operator");
    }
    if (!isValidVulnerabilitySeverityValue(enumPredicate.getValue())) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid Vulnerability Severity Value");
    }
    return Status.OK;
  }

  private boolean isValidVulnerabilitySeverityValue(VulnerabilitySeverityValue value) {
    return !(value.equals(VulnerabilitySeverityValue.VULNERABILITY_SEVERITY_VALUE_UNSPECIFIED)
        || value.equals(VulnerabilitySeverityValue.UNRECOGNIZED));
  }

  private Status validatePathParamTypePredicate(PathParamTypePredicate enumPredicate) {
    if (!isValidEnumOperator(enumPredicate.getOperator())) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid Enum Operator");
    }
    if (!isValidPathParamTypeValue(enumPredicate.getValue())) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid Path Param Value");
    }
    return Status.OK;
  }

  private boolean isValidPathParamTypeValue(PathParamTypeValue value) {
    return !(value.equals(PathParamTypeValue.PATH_PARAM_TYPE_VALUE_UNSPECIFIED)
        || value.equals(PathParamTypeValue.UNRECOGNIZED));
  }

  private boolean isValidEnumOperator(EnumOperator operator) {
    return !(operator.equals(EnumOperator.ENUM_OPERATOR_UNSPECIFIED)
        || operator.equals(EnumOperator.UNRECOGNIZED));
  }

  private Status validateIntegerPredicate(IntPredicate intPredicate) {
    if (!isValidIntOperator(intPredicate.getOperator())) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid Integer Operator");
    }
    return Status.OK;
  }

  private boolean isValidIntOperator(IntOperator operator) {
    return !(operator.equals(IntOperator.INT_OPERATOR_UNSPECIFIED)
        || operator.equals(IntOperator.UNRECOGNIZED));
  }

  private Status validateStringPredicate(StringPredicate stringPredicate) {
    if (!isValidStringOperator(stringPredicate.getOperator())) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid String Operator");
    }
    if (!isValidStringValue(stringPredicate.getValue())) {
      return Status.INVALID_ARGUMENT.withDescription("Invalid String Value");
    }
    return Status.OK;
  }

  private boolean isValidStringValue(String value) {
    return !value.isBlank();
  }

  private boolean isValidStringOperator(StringOperator operator) {
    return !(operator.equals(StringOperator.STRING_OPERATOR_UNSPECIFIED)
        || operator.equals(StringOperator.UNRECOGNIZED));
  }
}
