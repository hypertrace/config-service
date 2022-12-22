package ai.traceable.integration.config.service.snyk.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.integration.config.service.snyk.v1.CreateSnykIntegrationRequest;
import ai.traceable.integration.config.service.snyk.v1.DeleteSnykIntegrationRequest;
import ai.traceable.integration.config.service.snyk.v1.EncryptedText;
import ai.traceable.integration.config.service.snyk.v1.GetSnykIntegrationDetailsRequest;
import ai.traceable.integration.config.service.snyk.v1.GetSnykIntegrationSummaryRequest;
import ai.traceable.integration.config.service.snyk.v1.UpdateSnykIntegrationRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class SnykIntegrationConfigRequestValidator {

  public void validateOrThrow(
      RequestContext requestContext, GetSnykIntegrationSummaryRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(
      RequestContext requestContext, GetSnykIntegrationDetailsRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateOrThrow(RequestContext requestContext, CreateSnykIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, CreateSnykIntegrationRequest.NAME_FIELD_NUMBER);
    validateEncryptedText(request.getApiToken());
  }

  public void validateOrThrow(RequestContext requestContext, UpdateSnykIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateSnykIntegrationRequest.NAME_FIELD_NUMBER);
    if (request.hasApiToken()) {
      validateEncryptedText(request.getApiToken());
    }
  }

  public void validateOrThrow(RequestContext requestContext, DeleteSnykIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateEncryptedText(EncryptedText encryptedText) {
    validateNonDefaultPresenceOrThrow(encryptedText, EncryptedText.KEY_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(encryptedText, EncryptedText.VALUE_FIELD_NUMBER);
  }
}
