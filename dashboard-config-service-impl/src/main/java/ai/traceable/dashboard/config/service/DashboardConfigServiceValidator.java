package ai.traceable.dashboard.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.dashboard.config.service.v1.CreateDashboardRequest;
import ai.traceable.dashboard.config.service.v1.DeleteDashboardRequest;
import ai.traceable.dashboard.config.service.v1.UpdateDashboardRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DashboardConfigServiceValidator {

  public void validate(RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validate(RequestContext requestContext, CreateDashboardRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, CreateDashboardRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, CreateDashboardRequest.JSON_FIELD_NUMBER);
  }

  public void validate(RequestContext requestContext, UpdateDashboardRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateDashboardRequest.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, UpdateDashboardRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, UpdateDashboardRequest.JSON_FIELD_NUMBER);
  }

  public void validate(RequestContext requestContext, DeleteDashboardRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateDashboardRequest.ID_FIELD_NUMBER);
  }
}
