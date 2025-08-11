package ai.traceable.integration.config.service.snyk.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.integration.config.service.snyk.v1.CreateSnykIntegrationRequest;
import ai.traceable.integration.config.service.snyk.v1.DeleteSnykIntegrationRequest;
import ai.traceable.integration.config.service.snyk.v1.EncryptedText;
import ai.traceable.integration.config.service.snyk.v1.GetSnykIntegrationDetailsRequest;
import ai.traceable.integration.config.service.snyk.v1.GetSnykIntegrationSummaryRequest;
import ai.traceable.integration.config.service.snyk.v1.SnykBaseUrls;
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
    validateEncryptedText(request.getApiToken());
    // validation for Snyk endpoints to be added after UI catches up
  }

  public void validateOrThrow(RequestContext requestContext, UpdateSnykIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateEncryptedText(request.getApiToken());
    // validation for Snyk endpoints to be added after UI catches up
  }

  public void validateOrThrow(RequestContext requestContext, DeleteSnykIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
  }

  public void validateEncryptedText(EncryptedText encryptedText) {
    validateNonDefaultPresenceOrThrow(encryptedText, EncryptedText.KEY_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(encryptedText, EncryptedText.VALUE_FIELD_NUMBER);
  }

  private void validateSnykEndpoints(SnykBaseUrls baseUrls) {
    validateNonDefaultPresenceOrThrow(baseUrls, SnykBaseUrls.API_BASE_URL_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(baseUrls, SnykBaseUrls.APP_BASE_URL_FIELD_NUMBER);
  }
}
