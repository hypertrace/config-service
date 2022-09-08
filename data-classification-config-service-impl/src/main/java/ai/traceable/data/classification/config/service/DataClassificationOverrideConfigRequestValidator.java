package ai.traceable.data.classification.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.data.classification.config.service.v1.CreateDataClassificationOverrideRequest;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideFilter;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DeleteDataClassificationOverridesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataClassificationOverridesRequest;
import ai.traceable.data.classification.config.service.v1.IdFilter;
import ai.traceable.data.classification.config.service.v1.UpdateDataClassificationOverrideRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DataClassificationOverrideConfigRequestValidator {

  public void validateOrThrow(
      RequestContext requestContext, CreateDataClassificationOverrideRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateDataClassificationOverrideRule(request.getDataClassificationOverrideRule());
  }

  public void validateOrThrow(
      RequestContext requestContext, GetDataClassificationOverridesRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.hasFilter()) {
      validateDataClassificationOverrideFilter(request.getFilter());
    }
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateDataClassificationOverrideRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, UpdateDataClassificationOverrideRequest.ID_FIELD_NUMBER);
    validateDataClassificationOverrideRule(request.getDataClassificationOverrideRule());
  }

  public void validateOrThrow(
      RequestContext requestContext, DeleteDataClassificationOverridesRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.hasFilter()) {
      validateDataClassificationOverrideFilter(request.getFilter());
    }
  }

  private void validateDataClassificationOverrideRule(DataClassificationOverrideRule rule) {
    validateScope(rule.getScope());
    validateOverride(rule);
  }

  private void validateScope(DataClassificationOverrideRule.DataClassificationOverrideScope scope) {
    switch (scope.getScopeCase()) {
      case ENVIRONMENT_SCOPE:
        validateEnvironmentScope(scope);
        break;
      case SCOPE_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected scope case:" + scope.getScopeCase())
            .asRuntimeException();
    }
  }

  private void validateOverride(DataClassificationOverrideRule rule) {
    switch (rule.getOverrideCase()) {
      case DATA_SUPPRESSION_OVERRIDE:
        validateDataSupressionOverride(rule);
        break;
      case OVERRIDE_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected suppression case:" + rule.getOverrideCase())
            .asRuntimeException();
    }
  }

  private void validateDataClassificationOverrideFilter(DataClassificationOverrideFilter filter) {
    switch (filter.getFilterCase()) {
      case ID_FILTER:
        validateNonDefaultPresenceOrThrow(filter.getIdFilter(), IdFilter.IDS_FIELD_NUMBER);
        return;
      case SCOPE_FILTER:
        validateScopeFilter(filter);
        return;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected filter case:" + filter.getFilterCase())
            .asRuntimeException();
    }
  }

  private void validateEnvironmentScope(
      DataClassificationOverrideRule.DataClassificationOverrideScope scope) {
    validateNonDefaultPresenceOrThrow(
        scope.getEnvironmentScope(),
        DataClassificationOverrideRule.EnvironmentScope.ENVIRONMENT_ID_FIELD_NUMBER);
  }

  private void validateDataSupressionOverride(DataClassificationOverrideRule rule) {
    DataSetInfo.DataSuppression suppression =
        rule.getDataSuppressionOverride().getDataSuppression();
    if (suppression.equals(DataSetInfo.DataSuppression.UNRECOGNIZED)
        || suppression.equals(DataSetInfo.DataSuppression.DATA_SUPPRESSION_UNSPECIFIED)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Unexpected Suppression:" + suppression)
          .asRuntimeException();
    }
  }

  private void validateScopeFilter(DataClassificationOverrideFilter filter) {
    filter
        .getScopeFilter()
        .getScopesList()
        .forEach(
            dataClassificationOverrideScope -> {
              validateScope(dataClassificationOverrideScope);
            });
  }
}
