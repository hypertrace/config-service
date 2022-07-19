package ai.traceable.api.spec.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.api.spec.config.service.v1.CreateApiSpec;
import ai.traceable.api.spec.config.service.v1.CreateApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.DeleteApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.GetApiSpecRequest;
import ai.traceable.api.spec.config.service.v1.GetApiSpecsRequest;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpec;
import ai.traceable.api.spec.config.service.v1.UpdateApiSpecRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ApiSpecConfigRequestValidator {

  public void validateOrThrow(RequestContext requestContext, GetApiSpecsRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(RequestContext requestContext, GetApiSpecRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, GetApiSpecRequest.ID_FIELD_NUMBER);
  }

  public void validateOrThrow(RequestContext requestContext, CreateApiSpecRequest request) {
    validateRequestContextOrThrow(requestContext);
    CreateApiSpec createApiSpec = request.getCreateApiSpec();
    validateNonDefaultPresenceOrThrow(createApiSpec, CreateApiSpec.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(createApiSpec, CreateApiSpec.STATUS_FIELD_NUMBER);
  }

  public void validateOrThrow(RequestContext requestContext, UpdateApiSpecRequest request) {
    validateRequestContextOrThrow(requestContext);
    this.validateUpdateApiSpec(request.getApiSpec());
  }

  public void validateOrThrow(RequestContext requestContext, DeleteApiSpecRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteApiSpecRequest.SPEC_ID_FIELD_NUMBER);
  }

  private void validateUpdateApiSpec(UpdateApiSpec updateApiSpec) {
    validateNonDefaultPresenceOrThrow(updateApiSpec, UpdateApiSpec.SPEC_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(updateApiSpec, UpdateApiSpec.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(updateApiSpec, UpdateApiSpec.STATUS_FIELD_NUMBER);
  }
}
