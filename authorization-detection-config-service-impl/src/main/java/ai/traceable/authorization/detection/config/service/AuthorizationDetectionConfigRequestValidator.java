package ai.traceable.authorization.detection.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.authorization.detection.config.service.v1.GetAuthorizationDetectionRulesRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

class AuthorizationDetectionConfigRequestValidator {
  public void validateOrThrow(
      RequestContext requestContext, GetAuthorizationDetectionRulesRequest request) {
    validateRequestContextOrThrow(requestContext);
  }
}
