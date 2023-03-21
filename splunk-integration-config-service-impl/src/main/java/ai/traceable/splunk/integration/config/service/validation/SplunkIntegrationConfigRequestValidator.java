package ai.traceable.splunk.integration.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.splunk.integration.config.service.api.v1.*;
import ai.traceable.splunk.integration.config.service.store.SplunkIntegrationConfigStore;
import com.google.inject.Inject;
import io.grpc.Status;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class SplunkIntegrationConfigRequestValidator {

  private final SplunkIntegrationConfigStore splunkIntegrationConfigStore;

  public void validateOrThrow(
      RequestContext requestContext, CreateSplunkIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, CreateSplunkIntegrationRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, CreateSplunkIntegrationRequest.HTTP_EVENT_COLLECTOR_URL_FIELD_NUMBER);
    validateEncryptedText(request.getApiToken());
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateSplunkIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateSplunkIntegrationRequest.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, UpdateSplunkIntegrationRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, UpdateSplunkIntegrationRequest.HTTP_EVENT_COLLECTOR_URL_FIELD_NUMBER);
    validateEncryptedText(request.getApiToken());

    // let us check if the id exists in DB or not
    boolean exists =
        splunkIntegrationConfigStore.getObject(requestContext, request.getId()).isPresent();
    if (!exists) {
      throw Status.NOT_FOUND
          .withDescription(
              String.format(
                  "Attempting to update an object that does not exist for id {%s}",
                  request.getId()))
          .asRuntimeException();
    }
  }

  public void validateOrThrow(RequestContext requestContext, GetSplunkIntegrationsRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.hasFilter()) {
      validateNonDefaultPresenceOrThrow(
          request.getFilter(), SplunkIntegrationsFilter.IDS_FIELD_NUMBER);
    }
  }

  public void validateOrThrow(
      RequestContext requestContext, DeleteSplunkIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteSplunkIntegrationRequest.ID_FIELD_NUMBER);
  }

  public void validateEncryptedText(EncryptedText encryptedText) {
    validateNonDefaultPresenceOrThrow(encryptedText, EncryptedText.KEY_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(encryptedText, EncryptedText.VALUE_FIELD_NUMBER);
  }
}
