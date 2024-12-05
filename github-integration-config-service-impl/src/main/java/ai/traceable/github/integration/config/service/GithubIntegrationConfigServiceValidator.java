package ai.traceable.github.integration.config.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.github.integration.config.service.v1.GetGithubIntegrationsRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

class GithubIntegrationConfigServiceValidator {
  void validateGetGithubIntegrations(
      GetGithubIntegrationsRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
  }
}
