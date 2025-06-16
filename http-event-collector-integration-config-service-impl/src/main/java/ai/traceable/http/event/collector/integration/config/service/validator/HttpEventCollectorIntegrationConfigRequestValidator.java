package ai.traceable.http.event.collector.integration.config.service.validator;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.http.event.collector.integration.config.service.store.HttpEventCollectorIntegrationConfigServiceStore;
import ai.traceable.http.event.collector.integration.config.service.v1.CreateHttpEventCollectorIntegrationRequest;
import ai.traceable.http.event.collector.integration.config.service.v1.DeleteHttpEventCollectorIntegrationRequest;
import ai.traceable.http.event.collector.integration.config.service.v1.EncryptedText;
import ai.traceable.http.event.collector.integration.config.service.v1.GetHttpEventCollectorIntegrationsRequest;
import ai.traceable.http.event.collector.integration.config.service.v1.HttpEventCollectorIntegrationsFilter;
import ai.traceable.http.event.collector.integration.config.service.v1.UpdateHttpEventCollectorIntegrationRequest;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.net.MalformedURLException;
import java.net.URL;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class HttpEventCollectorIntegrationConfigRequestValidator {

  private final HttpEventCollectorIntegrationConfigServiceStore store;

  public void validateOrThrow(
      RequestContext requestContext, CreateHttpEventCollectorIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, CreateHttpEventCollectorIntegrationRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, CreateHttpEventCollectorIntegrationRequest.HTTP_EVENT_COLLECTOR_URL_FIELD_NUMBER);
    validateEventCollectorURL(request.getHttpEventCollectorUrl());
    validateEncryptedText(request.getApiToken());
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateHttpEventCollectorIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, UpdateHttpEventCollectorIntegrationRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, UpdateHttpEventCollectorIntegrationRequest.HTTP_EVENT_COLLECTOR_URL_FIELD_NUMBER);
    validateEventCollectorURL(request.getHttpEventCollectorUrl());

    // let us check if the id exists in DB or not
    boolean exists = store.getObject(requestContext, request.getId()).isPresent();
    if (!exists) {
      throw Status.NOT_FOUND
          .withDescription(
              String.format(
                  "Attempting to update an object that does not exist for id {%s}",
                  request.getId()))
          .asRuntimeException();
    }
  }

  public void validateOrThrow(
      RequestContext requestContext, GetHttpEventCollectorIntegrationsRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.hasFilter()) {
      validateNonDefaultPresenceOrThrow(
          request.getFilter(), HttpEventCollectorIntegrationsFilter.IDS_FIELD_NUMBER);
    }
  }

  public void validateOrThrow(
      RequestContext requestContext, DeleteHttpEventCollectorIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, DeleteHttpEventCollectorIntegrationRequest.ID_FIELD_NUMBER);
  }

  void validateEventCollectorURL(String urlString) {
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

  public void validateEncryptedText(EncryptedText encryptedText) {
    validateNonDefaultPresenceOrThrow(encryptedText, EncryptedText.KEY_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(encryptedText, EncryptedText.VALUE_FIELD_NUMBER);
  }
}
