package ai.traceable.agent.action.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.agent.action.config.service.v1.AgentActionDetails;
import ai.traceable.agent.action.config.service.v1.AgentActionInput;
import ai.traceable.agent.action.config.service.v1.CreateAgentActionRequest;
import ai.traceable.agent.action.config.service.v1.DeleteAgentActionRequest;
import ai.traceable.agent.action.config.service.v1.GetAgentActionsRequest;
import ai.traceable.agent.action.config.service.v1.UpdateAgentActionRequest;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class AgentActionRequestValidator {

  public void validateOrThrow(RequestContext requestContext, CreateAgentActionRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (!request.hasAction()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing agent action inside create request")
          .asRuntimeException();
    }
    validateAgentActionInput(request.getAction());
  }

  public void validateOrThrow(RequestContext requestContext, GetAgentActionsRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (!request.hasFilter()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing filter inside agent action get request")
          .asRuntimeException();
    }
  }

  public void validateOrThrow(RequestContext requestContext, UpdateAgentActionRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateAgentActionRequest.ID_FIELD_NUMBER);
    validateAgentActionInput(request.getAction());
  }

  public void validateOrThrow(RequestContext requestContext, DeleteAgentActionRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteAgentActionRequest.ID_FIELD_NUMBER);
  }

  private void validateAgentActionInput(AgentActionInput input) {
    if (input.getDetailsCount() == 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("No actions provided in agent action input")
          .asRuntimeException();
    }

    for (AgentActionDetails detail : input.getDetailsList()) {
      if (!detail.hasRestartAgentAction()
          && !detail.hasUpdateConfigAction()
          && !detail.hasFetchDebugInformationAction()) {
        throw Status.INVALID_ARGUMENT
            .withDescription("No action detail provided in agent action input")
            .asRuntimeException();
      }
    }
  }
}
