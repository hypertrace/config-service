package ai.traceable.github.integration.config.service;

import static ai.traceable.github.integration.config.service.v1.IntegrationStatus.StatusCase.AWAITING_APPROVAL;
import static ai.traceable.github.integration.config.service.v1.IntegrationStatus.StatusCase.AWAITING_REQUEST;
import static ai.traceable.github.integration.config.service.v1.IntegrationStatus.StatusCase.COMPLETED;
import static ai.traceable.github.integration.config.service.v1.IntegrationStatus.StatusCase.SUSPENDED;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.github.integration.config.service.v1.CreateGithubIntegrationRequest;
import ai.traceable.github.integration.config.service.v1.DeleteGithubIntegrationRequest;
import ai.traceable.github.integration.config.service.v1.GetGithubIntegrationsRequest;
import ai.traceable.github.integration.config.service.v1.IntegrationStatus;
import ai.traceable.github.integration.config.service.v1.IntegrationStatus.AwaitingApproval;
import ai.traceable.github.integration.config.service.v1.IntegrationStatus.Completed;
import ai.traceable.github.integration.config.service.v1.IntegrationStatus.StatusCase;
import ai.traceable.github.integration.config.service.v1.IntegrationStatus.Suspended;
import ai.traceable.github.integration.config.service.v1.UpdateGithubIntegrationRequest;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import org.hypertrace.core.grpcutils.context.RequestContext;

class GithubIntegrationConfigServiceValidator {
  private static final SetMultimap<StatusCase, StatusCase> ALLOWED_STATUS_TRANSITIONS =
      ImmutableSetMultimap.of(
          AWAITING_REQUEST, AWAITING_APPROVAL,
          AWAITING_REQUEST, COMPLETED,
          AWAITING_APPROVAL, COMPLETED,
          COMPLETED, SUSPENDED,
          SUSPENDED, COMPLETED);

  void validateGetGithubIntegrations(
      GetGithubIntegrationsRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
  }

  void validateDeleteGithubIntegration(
      DeleteGithubIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteGithubIntegrationRequest.ID_FIELD_NUMBER);
  }

  void validateCreateGithubIntegration(
      CreateGithubIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
  }

  void validateUpdateGithubIntegration(
      UpdateGithubIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateGithubIntegrationRequest.ID_FIELD_NUMBER);
    switch (request.getStatus().getStatusCase()) {
      case AWAITING_APPROVAL:
        validateNonDefaultPresenceOrThrow(
            request.getStatus().getAwaitingApproval(),
            AwaitingApproval.GITHUB_INSTALLATION_TARGET_NAME_FIELD_NUMBER);
        return;
      case COMPLETED:
        validateNonDefaultPresenceOrThrow(
            request.getStatus().getCompleted(), Completed.INSTALLATION_ID_FIELD_NUMBER);
        validateNonDefaultPresenceOrThrow(
            request.getStatus().getCompleted(),
            Completed.GITHUB_INSTALLATION_TARGET_NAME_FIELD_NUMBER);
        validateNonDefaultPresenceOrThrow(
            request.getStatus().getCompleted(), Completed.GITHUB_INSTALLATION_URL_FIELD_NUMBER);
        return;
      case SUSPENDED:
        validateNonDefaultPresenceOrThrow(
            request.getStatus().getSuspended(), Suspended.INSTALLATION_ID_FIELD_NUMBER);
        validateNonDefaultPresenceOrThrow(
            request.getStatus().getSuspended(),
            Suspended.GITHUB_INSTALLATION_TARGET_NAME_FIELD_NUMBER);
        validateNonDefaultPresenceOrThrow(
            request.getStatus().getSuspended(), Suspended.GITHUB_INSTALLATION_URL_FIELD_NUMBER);
      case AWAITING_REQUEST: // Not updatable, this is the initial state
      default:
        throw io.grpc.Status.INVALID_ARGUMENT
            .withDescription(
                String.format("%s is not supported as an update state ", request.getStatus()))
            .asRuntimeException();
    }
  }

  void validateUpdateValidForStatusChange(
      IntegrationStatus previousState, IntegrationStatus newState) {
    if (!ALLOWED_STATUS_TRANSITIONS
        .get(previousState.getStatusCase())
        .contains(newState.getStatusCase())) {
      throw io.grpc.Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid state transition from %s to %s",
                  previousState.getStatusCase(), newState.getStatusCase()))
          .asRuntimeException();
    }
  }
}
