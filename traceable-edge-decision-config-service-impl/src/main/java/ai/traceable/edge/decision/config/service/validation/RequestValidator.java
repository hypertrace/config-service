package ai.traceable.edge.decision.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import org.hypertrace.core.grpcutils.context.RequestContext;

public class RequestValidator {
  public static void validateRequestContext(RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
  }
}
