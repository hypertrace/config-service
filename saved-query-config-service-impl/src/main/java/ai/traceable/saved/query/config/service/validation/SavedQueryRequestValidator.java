package ai.traceable.saved.query.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.saved.query.config.service.v1.CreateSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.DeleteSavedQueryRequest;
import ai.traceable.saved.query.config.service.v1.GetSavedQueriesRequest;
import ai.traceable.saved.query.config.service.v1.QueryClauses;
import ai.traceable.saved.query.config.service.v1.UpdateSavedQueryRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class SavedQueryRequestValidator {

  public void validateOrThrow(RequestContext requestContext, CreateSavedQueryRequest request) {
    validateRequestContext(requestContext);
    validateNonDefaultPresenceOrThrow(request, CreateSavedQueryRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, CreateSavedQueryRequest.SCOPE_FIELD_NUMBER);
    validateQueryClauses(request.getQueryClauses());
  }

  public void validateOrThrow(RequestContext requestContext, UpdateSavedQueryRequest request) {
    validateRequestContext(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateSavedQueryRequest.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, UpdateSavedQueryRequest.NAME_FIELD_NUMBER);
    validateQueryClauses(request.getQueryClauses());
  }

  public void validateOrThrow(RequestContext requestContext, DeleteSavedQueryRequest request) {
    validateRequestContext(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteSavedQueryRequest.ID_FIELD_NUMBER);
  }

  public void validateOrThrow(RequestContext requestContext, GetSavedQueriesRequest request) {
    validateRequestContext(requestContext);
  }

  private static void validateQueryClauses(QueryClauses queryClauses) {
    validateNonDefaultPresenceOrThrow(queryClauses, QueryClauses.SELECTION_FIELD_NUMBER);
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
