package ai.traceable.data.classification.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.data.classification.config.service.v1.CreateDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ApiScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.EnvironmentScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.KeyValuePattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.data.classification.config.service.v1.DeleteDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.UpdateDataTypeRequest;
import io.grpc.Status;
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
  }

  private void validateScopedPatternList(DataTypeRule rule) {
    for (ScopedPattern scopedPattern : rule.getScopedPatternList()) {
      validateScopedPattern(scopedPattern);
    }
  }

  private void validateScopedPattern(ScopedPattern scopedPattern) {
    validateScope(scopedPattern);
    validateNonDefaultPresenceOrThrow(scopedPattern, ScopedPattern.PARAMETER_TYPE_FIELD_NUMBER);
    validatePattern(scopedPattern);
    validateNonDefaultPresenceOrThrow(scopedPattern, ScopedPattern.ACTION_FIELD_NUMBER);
  }

  private void validatePattern(ScopedPattern scopedPattern) {
    switch (scopedPattern.getPatternCase()) {
      case KEY_PATTERN:
        this.validateStringPattern(scopedPattern.getKeyPattern());
        break;
      case KEY_VALUE_PATTERN:
        this.validateKeyValuePattern(scopedPattern.getKeyValuePattern());
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
}
