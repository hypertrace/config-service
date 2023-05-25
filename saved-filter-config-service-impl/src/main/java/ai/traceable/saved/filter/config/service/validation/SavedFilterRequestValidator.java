package ai.traceable.saved.filter.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.saved.filter.config.service.v1.CreateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.DeleteSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.FilterCriteria;
import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersRequest;
import ai.traceable.saved.filter.config.service.v1.LogicalFilterCondition;
import ai.traceable.saved.filter.config.service.v1.RelationalFilterCondition;
import ai.traceable.saved.filter.config.service.v1.UpdateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.Visibility;
import io.grpc.Status;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class SavedFilterRequestValidator {

  public void validateOrThrow(RequestContext requestContext, CreateSavedFilterRequest request) {
    validateRequestContext(requestContext);
    validateNonDefaultPresenceOrThrow(request, CreateSavedFilterRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, CreateSavedFilterRequest.SCOPE_FIELD_NUMBER);
    validateVisibility(request.getVisibility());
    validateFilterCriteria(request.getFilterCriteria());
  }

  public void validateOrThrow(RequestContext requestContext, UpdateSavedFilterRequest request) {
    validateRequestContext(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateSavedFilterRequest.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, UpdateSavedFilterRequest.NAME_FIELD_NUMBER);
    validateVisibility(request.getVisibility());
    validateFilterCriteria(request.getFilterCriteria());
  }

  public void validateOrThrow(RequestContext requestContext, DeleteSavedFilterRequest request) {
    validateRequestContext(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteSavedFilterRequest.ID_FIELD_NUMBER);
  }

  public void validateOrThrow(RequestContext requestContext, GetSavedFiltersRequest request) {
    validateRequestContext(requestContext);
    validateNonDefaultPresenceOrThrow(request, GetSavedFiltersRequest.SCOPE_FIELD_NUMBER);
  }

  private static void validateFilterCriteria(FilterCriteria filterCriteria) {
    switch (filterCriteria.getFilterConditionCase()) {
      case LOGICAL_FILTER:
        validateLogicalFilter(filterCriteria);
        break;
      case RELATIONAL_FILTER:
        validateRelationalFilter(filterCriteria);
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected filter condition: " + printMessage(filterCriteria))
            .asRuntimeException();
    }
  }

  private static void validateLogicalFilter(FilterCriteria filterCriteria) {
    LogicalFilterCondition logicalFilter = filterCriteria.getLogicalFilter();
    validateNonDefaultPresenceOrThrow(logicalFilter, LogicalFilterCondition.OPERATOR_FIELD_NUMBER);
    List<FilterCriteria> filterCriteriaList = logicalFilter.getFilterCriteriaList();
    if (filterCriteriaList.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Logical filter's criteria is empty")
          .asRuntimeException();
    }
    filterCriteriaList.forEach(SavedFilterRequestValidator::validateFilterCriteria);
  }

  private static void validateRelationalFilter(FilterCriteria filterCriteria) {
    RelationalFilterCondition relationalFilter = filterCriteria.getRelationalFilter();
    validateNonDefaultPresenceOrThrow(
        relationalFilter, RelationalFilterCondition.FIELD_NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        relationalFilter, RelationalFilterCondition.OPERATOR_FIELD_NUMBER);
    if (!relationalFilter.hasFieldValue()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Relational filter's field value is not set")
          .asRuntimeException();
    }
  }

  private static void validateVisibility(Visibility visibility) {
    switch (visibility.getVisibilityTypeCase()) {
      case PUBLIC:
      case PRIVATE:
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected visibility: " + printMessage(visibility))
            .asRuntimeException();
    }
  }

  private static void validateRequestContext(RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    if (requestContext.getUserId().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing expected User ID")
          .asRuntimeException();
    }
  }
}
