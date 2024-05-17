package ai.traceable.data.protection.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.data.protection.config.service.v1.DataClassifierCondition;
import ai.traceable.data.protection.config.service.v1.DataProtectionConfig;
import ai.traceable.data.protection.config.service.v1.DataProtectionExclusion;
import ai.traceable.data.protection.config.service.v1.DataProtectionExclusionCondition;
import ai.traceable.data.protection.config.service.v1.DataProtectionScope;
import ai.traceable.data.protection.config.service.v1.DataSensitivity;
import ai.traceable.data.protection.config.service.v1.DeleteScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.EntityCondition;
import ai.traceable.data.protection.config.service.v1.EntityType;
import ai.traceable.data.protection.config.service.v1.GetResolvedScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.ParameterNameCondition;
import ai.traceable.data.protection.config.service.v1.UpsertScopedDataProtectionConfigRequest;
import io.grpc.Status;
import java.util.List;
import org.hypertrace.core.grpcutils.context.ContextualStatusExceptionBuilder;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DataProtectionConfigValidator implements ConfigValidator {
  public void validateGetRequest(
      RequestContext requestContext, GetResolvedScopedDataProtectionConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.hasScope()) {
      validateScope(request.getScope());
    }
  }

  public void validateUpsertRequest(
      RequestContext requestContext, UpsertScopedDataProtectionConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateDataProtectionConfig(request.getConfig());
    if (request.hasScope()) {
      validateScope(request.getScope());
    }
  }

  public void validateDeleteRequest(
      RequestContext requestContext, DeleteScopedDataProtectionConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.hasScope()) {
      validateScope(request.getScope());
    }
  }

  private void validateDataProtectionConfig(DataProtectionConfig config) {
    if (config.getMinDataSensitivity() == DataSensitivity.DATA_SENSITIVITY_UNSPECIFIED
        || config.getMinDataSensitivity() == DataSensitivity.UNRECOGNIZED) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INVALID_ARGUMENT.withDescription(
                  String.format(
                      "Invalid min data sensitivity %s for data protection config",
                      config.getMinDataSensitivity())))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
    validateDataProtectionExclusionList(config.getExclusionsList());
  }

  private void validateDataProtectionExclusionList(
      List<DataProtectionExclusion> dataProtectionExclusionList) {
    dataProtectionExclusionList.forEach(
        dataProtectionExclusion ->
            validateDataProtectionExclusionConditionList(
                dataProtectionExclusion.getConditionsList()));
  }

  private void validateDataProtectionExclusionConditionList(
      List<DataProtectionExclusionCondition> conditions) {
    conditions.forEach(
        condition -> {
          switch (condition.getConditionCase()) {
            case ENTITY_CONDITION:
              validateEntityCondition(condition.getEntityCondition());
              break;
            case DATA_CLASSIFIER_CONDITION:
              validateDataClassifierCondition(condition.getDataClassifierCondition());
              break;
            case PARAM_NAME_CONDITION:
              validateParamNameCondition(condition.getParamNameCondition());
              break;
            default:
              throw Status.INVALID_ARGUMENT
                  .withDescription("Invalid data protection exclusion condition")
                  .asRuntimeException();
          }
        });
  }

  private void validateEntityCondition(EntityCondition entityCondition) {
    if (entityCondition.getEntityType() == EntityType.ENTITY_TYPE_UNSPECIFIED
        || entityCondition.getEntityType() == EntityType.UNRECOGNIZED) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid entity type condition %s for data protection config",
                  entityCondition.getEntityType()))
          .asRuntimeException();
    }
    if (entityCondition.getEntityIdsCount() == 0) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INVALID_ARGUMENT.withDescription("Entity ID list cannot be empty"))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
  }

  private void validateDataClassifierCondition(DataClassifierCondition dataClassifierCondition) {
    if (dataClassifierCondition.getDatasetIdsCount() == 0
        && dataClassifierCondition.getDatatypeIdsCount() == 0) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INVALID_ARGUMENT.withDescription(
                  "Either dataset ID or datatype ID list should be non empty"))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
  }

  private void validateParamNameCondition(ParameterNameCondition parameterNameCondition) {
    if (parameterNameCondition.getParamNamesCount() == 0
        && parameterNameCondition.getParamNameRegexesCount() == 0) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INVALID_ARGUMENT.withDescription(
                  "Either param name or param name regex list should be non empty"))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
  }

  private void validateScope(DataProtectionScope scope) {
    if (scope.getScopeCase() == DataProtectionScope.ScopeCase.SCOPE_NOT_SET) {
      throw Status.INVALID_ARGUMENT.withDescription("Scope must be non empty").asRuntimeException();
    }
  }
}
