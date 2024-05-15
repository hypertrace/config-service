package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_UNSPECIFIED;
import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.UNRECOGNIZED;
import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateFieldPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.data.classification.config.service.v1.CreateDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Action;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ApiScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.EnvironmentScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.KeyValuePattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Location;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.UrlMatchScope;
import ai.traceable.data.classification.config.service.v1.DeleteDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.UpdateDataTypeRequest;
import io.grpc.Status;
import java.util.List;
import org.hypertrace.config.validation.RegexValidator;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DataTypeConfigRequestValidator {

  public void validateOrThrow(RequestContext requestContext, CreateDataTypeRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateDataTypeRule(request.getRule());
  }

  public void validateOrThrow(RequestContext requestContext, GetDataTypesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(RequestContext requestContext, UpdateDataTypeRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateDataTypeRequest.ID_FIELD_NUMBER);
    validateDataTypeRule(request.getRule());
  }

  public void validateOrThrow(RequestContext requestContext, DeleteDataTypeRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteDataTypeRequest.ID_FIELD_NUMBER);
  }

  private void validateDataTypeRule(DataTypeRule rule) {
    validateNonDefaultPresenceOrThrow(rule, DataTypeRule.NAME_FIELD_NUMBER);
    validateScopedPatternList(rule);
    if (rule.hasSuppressionPattern() && rule.getSuppressionPattern().isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "suppression pattern cannot be empty if specified: " + printMessage(rule))
          .asRuntimeException();
    }
    if (rule.getDataSetIdCount() > 0) {
      // If it contains its own data set id references, it's been migrated and should have all
      // optional fields inherited from data set assigned
      validateNonDefaultPresenceOrThrow(rule, DataTypeRule.SENSITIVITY_FIELD_NUMBER);
      validateFieldPresenceOrThrow(rule, DataTypeRule.ENABLED_FIELD_NUMBER);
      validateNonDefaultPresenceOrThrow(rule, DataTypeRule.DATA_SUPPRESSION_FIELD_NUMBER);
    }
  }

  private void validateScopedPatternList(DataTypeRule rule) {
    List<ScopedPattern> scopedPatternList = rule.getScopedPatternsList();
    if (scopedPatternList.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("scoped pattern cannot be empty: " + printMessage(rule))
          .asRuntimeException();
    }
    boolean isAllIgnorePatterns = true;
    for (ScopedPattern scopedPattern : scopedPatternList) {
      validateScopedPattern(scopedPattern);
      if (scopedPattern.getAction().equals(Action.ACTION_MATCH)) {
        isAllIgnorePatterns = false;
      }
    }
    if (isAllIgnorePatterns) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "A data type rule pattern should have at least one match pattern: "
                  + printMessage(rule))
          .asRuntimeException();
    }
  }

  private void validateScopedPattern(ScopedPattern scopedPattern) {
    validateScope(scopedPattern);
    validateLocations(scopedPattern);
    validatePattern(scopedPattern);
    validateNonDefaultPresenceOrThrow(scopedPattern, ScopedPattern.ACTION_FIELD_NUMBER);
  }

  private void validateLocations(ScopedPattern scopedPattern) {
    List<Location> locationsList = scopedPattern.getLocationsList();
    if (locationsList.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("locations cannot be empty: " + printMessage(scopedPattern))
          .asRuntimeException();
    }
    locationsList.forEach(
        location -> {
          if (location.equals(UNRECOGNIZED) || location.equals(LOCATION_UNSPECIFIED)) {
            throw Status.INVALID_ARGUMENT
                .withDescription("Invalid location: " + printMessage(scopedPattern))
                .asRuntimeException();
          }
        });
  }

  private void validatePattern(ScopedPattern scopedPattern) {
    switch (scopedPattern.getPatternCase()) {
      case KEY_PATTERN:
        this.validateStringPattern(scopedPattern.getKeyPattern());
        break;
      case KEY_VALUE_PATTERN:
        this.validateKeyValuePattern(scopedPattern.getKeyValuePattern());
        break;
      case LEAF_KEY_VALUE_PATTERN:
        this.validateKeyValuePattern(scopedPattern.getLeafKeyValuePattern());
        break;
      case LEAF_KEY_PATTERN:
        this.validateStringPattern(scopedPattern.getLeafKeyPattern());
        break;
      case PATTERN_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected pattern case: " + printMessage(scopedPattern))
            .asRuntimeException();
    }
  }

  private void validateKeyValuePattern(KeyValuePattern pattern) {
    validateStringPattern(pattern.getKeyPattern());
    validateStringPattern(pattern.getValuePattern());
  }

  private void validateStringPattern(StringPattern pattern) {
    validateNonDefaultPresenceOrThrow(pattern, StringPattern.OPERATOR_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(pattern, StringPattern.VALUE_FIELD_NUMBER);
    if (pattern.getOperator().equals(DataTypeRule.Operator.OPERATOR_MATCHES_REGEX)) {
      Status regexValidationStatus = RegexValidator.validate(pattern.getValue());
      if (!regexValidationStatus.isOk()) {
        throw regexValidationStatus
            .withDescription(String.format("Invalid regex : %s", pattern.getValue()))
            .asRuntimeException();
      }
    }
  }

  private void validateScope(ScopedPattern scopedPattern) {
    switch (scopedPattern.getScopeCase()) {
      case GLOBAL_SCOPE:
        break;
      case ENVIRONMENT_SCOPE:
        this.validateEnvironmentScope(scopedPattern.getEnvironmentScope());
        break;
      case API_SCOPE:
        this.validateApiScope(scopedPattern.getApiScope());
        break;
      case SCOPE_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected scope case: " + printMessage(scopedPattern))
            .asRuntimeException();
    }
  }

  private void validateApiScope(ApiScope scope) {
    validateNonDefaultPresenceOrThrow(scope, ApiScope.API_IDS_FIELD_NUMBER);
  }

  private void validateEnvironmentScope(EnvironmentScope scope) {
    validateNonDefaultPresenceOrThrow(scope, EnvironmentScope.ENVIRONMENT_IDS_FIELD_NUMBER);
  }

  private void validateUrlMatchScope(UrlMatchScope scope) {
    validateNonDefaultPresenceOrThrow(scope, UrlMatchScope.URL_REGEX_MATCHES_FIELD_NUMBER);
    scope.getUrlRegexMatchesList().forEach(RegexValidator::validate);
  }
}
