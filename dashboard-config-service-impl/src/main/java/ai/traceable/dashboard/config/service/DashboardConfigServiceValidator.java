package ai.traceable.dashboard.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.dashboard.config.service.v1.CreateDashboardRequest;
import ai.traceable.dashboard.config.service.v1.Dashboard;
import ai.traceable.dashboard.config.service.v1.DeleteDashboardRequest;
import ai.traceable.dashboard.config.service.v1.UpdateDashboardRequest;
import ai.traceable.dashboard.config.service.v1.UpdateDashboardRoleAssignmentsRequest;
import io.grpc.Status;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.ContextualStatusExceptionBuilder;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = @Inject)
public class DashboardConfigServiceValidator {

  private final DashboardAccessUtils dashboardAccessUtils;

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

  public void validate(
      RequestContext requestContext, UpdateDashboardRoleAssignmentsRequest request) {
    validateNonDefaultPresenceOrThrow(
        request, UpdateDashboardRoleAssignmentsRequest.ID_FIELD_NUMBER);
    if (request.getRoleAssignments().getOwnersList().isEmpty()) {
      throw ContextualStatusExceptionBuilder.from(
              Status.INVALID_ARGUMENT.withDescription("Dashboard must have at least one owner"))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
  }

  public void validate(RequestContext requestContext, DeleteDashboardRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateDashboardRequest.ID_FIELD_NUMBER);
  }

  public void validateDeleteAccess(RequestContext requestContext, Dashboard existingDashboard) {
    String userEmail = requestContext.getEmail().orElseThrow();
    if (!this.dashboardAccessUtils.hasDeleteAccess(userEmail, existingDashboard)) {
      throw ContextualStatusExceptionBuilder.from(
              Status.PERMISSION_DENIED.withDescription(
                  "Only the dashboard owners can delete the dashboard"))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
  }

  public void validateUpdateAccess(RequestContext requestContext, Dashboard existingDashboard) {
    String userEmail = requestContext.getEmail().orElseThrow();
    if (!this.dashboardAccessUtils.hasUpdateAccess(userEmail, existingDashboard)) {
      throw ContextualStatusExceptionBuilder.from(
              Status.PERMISSION_DENIED.withDescription(
                  "You don't have permission to edit this dashboard"))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
  }

  public void validateRoleUpdateAccess(RequestContext requestContext, Dashboard existingDashboard) {
    String userEmail = requestContext.getEmail().orElseThrow();
    if (!this.dashboardAccessUtils.hasRoleUpdateAccess(userEmail, existingDashboard)) {
      throw ContextualStatusExceptionBuilder.from(
              Status.PERMISSION_DENIED.withDescription(
                  "You don't have permission to edit this dashboard"))
          .useStatusDescriptionAsExternalMessage()
          .buildRuntimeException();
    }
  }
}
