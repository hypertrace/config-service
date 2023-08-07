package ai.traceable.blocking.config.service.v2;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

class BlockingRulesRequestValidator {
  public void validateOrThrow(RequestContext requestContext, GetBlockingRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
    request.getRequestElementsList().forEach(this::validateRequestElement);
  }

  private void validateRequestElement(BlockingConfigRequestElement requestElement) {
    if (requestElement.getSupportedAgentCapabilitiesList().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Blocking request element should contain information about the agent capabilities")
          .asRuntimeException();
    }
  }
}
