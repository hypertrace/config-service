package ai.traceable.ipresolutionstrategy.config.service.v1;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class IpResolutionStrategyRequestValidator {
  public void validateOrThrow(
      RequestContext requestContext, CreateIpResolutionStrategyConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (!request.hasData()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing ip resolution strategy information inside create request")
          .asRuntimeException();
    }
    validateIpResolutionInput(request.getData());
  }

  public void validateOrThrow(
      RequestContext requestContext, GetIpResolutionStrategyConfigsRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (!request.hasFilter()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing filter inside ip resolution strategy get request")
          .asRuntimeException();
    }
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateIpResolutionStrategyConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, UpdateIpResolutionStrategyConfigRequest.ID_FIELD_NUMBER);
    validateIpResolutionInput(request.getData());
  }

  public void validateOrThrow(
      RequestContext requestContext, DeleteIpResolutionStrategyConfigRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, DeleteIpResolutionStrategyConfigRequest.ID_FIELD_NUMBER);
  }

  private void validateIpResolutionInput(IpResolutionStrategyConfigData input) {
    if (!input.hasStrategy()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("No strategy provided in ip resolution input")
          .asRuntimeException();
    }

    for (IpSource source : input.getStrategy().getSourcesList()) {
      if (!source.hasParsingStrategy()) {
        throw Status.INVALID_ARGUMENT
            .withDescription("No parsing strategy provided for one of the inputs")
            .asRuntimeException();
      }

      ParsingStrategy strategy = source.getParsingStrategy();
      if (!strategy.hasRawIp() && !strategy.hasKeywordBased() && !strategy.hasRegexBased()) {
        throw Status.INVALID_ARGUMENT
            .withDescription("Parsing strategy has no information on where to extract the ip from")
            .asRuntimeException();
      }
    }
  }
}
