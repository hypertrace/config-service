package ai.traceable.saved.filter.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.saved.filter.config.service.store.SavedFilterStoreManager;
import ai.traceable.saved.filter.config.service.v1.CreateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.DeleteSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.FilterCriteria;
import ai.traceable.saved.filter.config.service.v1.GetSavedFiltersRequest;
import ai.traceable.saved.filter.config.service.v1.SavedFilter;
import ai.traceable.saved.filter.config.service.v1.UpdateSavedFilterRequest;
import ai.traceable.saved.filter.config.service.v1.Visibility;
import ai.traceable.saved.filter.config.service.validation.SavedFilterValidator.ValidationContext;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class SavedFilterRequestValidator {
  SavedFilterValidator<FilterCriteria> savedFilterCriteriaValidator;
  private final SavedFilterStoreManager savedFilterStoreManager;

  public void validateOrThrow(RequestContext requestContext, CreateSavedFilterRequest request) {
    validateRequestContext(requestContext);
    validateNonDefaultPresenceOrThrow(request, CreateSavedFilterRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, CreateSavedFilterRequest.SCOPE_FIELD_NUMBER);
    validateVisibility(request.getVisibility());
    validateFilterCriteria(requestContext, request.getScope(), request.getFilterCriteria());
  }

  private void validateFilterCriteria(
      RequestContext requestContext, String scope, FilterCriteria filterCriteria) {

    ValidationContext validationContext =
        ValidationContext.builder().scope(scope).requestContext(requestContext).build();
    savedFilterCriteriaValidator.validate(filterCriteria, validationContext);
  }

  public void validateOrThrow(RequestContext requestContext, UpdateSavedFilterRequest request) {
    validateRequestContext(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateSavedFilterRequest.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, UpdateSavedFilterRequest.NAME_FIELD_NUMBER);
    validateVisibility(request.getVisibility());
    fetchAndValidateFilterCriteriaForUpdate(requestContext, request);
  }

  private void fetchAndValidateFilterCriteriaForUpdate(
      RequestContext requestContext, UpdateSavedFilterRequest request) {
    List<SavedFilter> savedFiltersFromDb =
        savedFilterStoreManager
            .fetchSavedFilters(
                requestContext, GetSavedFiltersRequest.newBuilder().setId(request.getId()).build())
            .getSavedFiltersList();

    if (savedFiltersFromDb.size() != 1) {
      throw Status.NOT_FOUND
          .withDescription(String.format("Filter with id %s is not found ", request.getId()))
          .asRuntimeException();
    }

    validateFilterCriteria(
        requestContext, savedFiltersFromDb.get(0).getScope(), request.getFilterCriteria());
  }

  public void validateOrThrow(RequestContext requestContext, DeleteSavedFilterRequest request) {
    validateRequestContext(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteSavedFilterRequest.ID_FIELD_NUMBER);
  }

  public void validateOrThrow(RequestContext requestContext, GetSavedFiltersRequest request) {
    validateRequestContext(requestContext);
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
