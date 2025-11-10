package ai.traceable.external.agent.action.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.external.agent.action.config.service.v1.GetAgentActionsRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ExternalAgentActionRequestValidator {
  public void validateOrThrow(RequestContext requestContext, GetAgentActionsRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request == null) {
      throw Status.INVALID_ARGUMENT.withDescription("Invalid request object").asRuntimeException();
    }

    if (request.getAgentActionRequestsList().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing expected Agent Action Requests")
          .asRuntimeException();
    }
  }
}
