package ai.traceable.splunk.integration.config.service.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.splunk.integration.config.service.api.v1.*;
import ai.traceable.splunk.integration.config.service.store.SplunkIntegrationConfigStore;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.grpc.Status;
import java.net.MalformedURLException;
import java.net.URL;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class SplunkIntegrationConfigRequestValidator {

  public static final String SPLUNK_HTTP_SUPPORT_ENABLED = "splunk.http.support.enabled";

  private final SplunkIntegrationConfigStore splunkIntegrationConfigStore;

  public void validateOrThrow(
      RequestContext requestContext, CreateSplunkIntegrationRequest request, Config config) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, CreateSplunkIntegrationRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, CreateSplunkIntegrationRequest.HTTP_EVENT_COLLECTOR_URL_FIELD_NUMBER);
    validateEventCollectorURL(request.getHttpEventCollectorUrl(), config);
    validateEncryptedText(request.getApiToken());
  }

  void validateEventCollectorURL(String eventCollectorUrl, Config config) {
    if (config == null
        || (config.hasPath(SPLUNK_HTTP_SUPPORT_ENABLED)
            && config.getBoolean(SPLUNK_HTTP_SUPPORT_ENABLED))) {
      return;
    }
    validateHttpsUrl(eventCollectorUrl);
  }

  private void validateHttpsUrl(String urlString) {
    try {
      URL url = new URL(urlString);
      String protocol = url.getProtocol();
      if (!protocol.equals("https")) {
        throw Status.INVALID_ARGUMENT
            .withDescription("URL configured in webhook is not https ")
            .asRuntimeException();
      }
    } catch (MalformedURLException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription("URL configured in webhook is malformed ")
          .asRuntimeException();
    }
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateSplunkIntegrationRequest request, Config config) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateSplunkIntegrationRequest.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, UpdateSplunkIntegrationRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, UpdateSplunkIntegrationRequest.HTTP_EVENT_COLLECTOR_URL_FIELD_NUMBER);
    validateEventCollectorURL(request.getHttpEventCollectorUrl(), config);

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
