package ai.traceable.blocking.config.service.v2;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

class BlockingRulesRequestValidator {
  public void validateOrThrow(RequestContext requestContext, GetBlockingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
    request.getRequestElementsList().forEach(this::validate);
  }

  private void validate(BlockingConfigRequestElement requestElement) {
    if (requestElement.getSupportedAgentCapabilitiesList().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Blocking request element should contain information about the agent capabilities")
          .asRuntimeException();
    }
    requestElement.getSupportedAgentCapabilitiesList().forEach(this::validate);
  }

  private void validate(AgentCapabilities agentCapabilities) {
    // Duplicates in component type are not allowed in same agent capabilities object
    if (agentCapabilities.getComponentsList().stream()
            .map(Component::getValueCase)
            .distinct()
            .count()
        != agentCapabilities.getComponentsList().size()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "A single Agent Capabilities object should not have multiple components of same type")
          .asRuntimeException();
    }
  }
}
