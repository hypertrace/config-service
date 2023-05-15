package ai.traceable.integration.config.service.wiz.validation;

import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.integration.config.service.wiz.store.WizIntegrationConfigStore;
import ai.traceable.integration.config.service.wiz.v1.CreateWizIntegrationRequest;
import ai.traceable.integration.config.service.wiz.v1.DeleteWizIntegrationRequest;
import ai.traceable.integration.config.service.wiz.v1.EncryptedText;
import ai.traceable.integration.config.service.wiz.v1.GetWizIntegrationSummariesRequest;
import ai.traceable.integration.config.service.wiz.v1.GetWizIntegrationsRequest;
import ai.traceable.integration.config.service.wiz.v1.UpdateWizIntegrationRequest;
import ai.traceable.integration.config.service.wiz.v1.WizIntegrationFilter;
import com.google.inject.Inject;
import io.grpc.Status;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class WizIntegrationConfigRequestValidator {

  private final WizIntegrationConfigStore wizIntegrationConfigStore;

  public void validateOrThrow(RequestContext requestContext, CreateWizIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, CreateWizIntegrationRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, CreateWizIntegrationRequest.CLIENT_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, UpdateWizIntegrationRequest.CLIENT_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, UpdateWizIntegrationRequest.TOKEN_URL_FIELD_NUMBER);

    validateEncryptedText(request.getClientSecret());
  }

  public void validateOrThrow(
      RequestContext requestContext, GetWizIntegrationSummariesRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.hasFilter()) {
      validateNonDefaultPresenceOrThrow(request.getFilter(), WizIntegrationFilter.IDS_FIELD_NUMBER);
    }
  }

  public void validateOrThrow(RequestContext requestContext, GetWizIntegrationsRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.hasFilter()) {
      validateNonDefaultPresenceOrThrow(request.getFilter(), WizIntegrationFilter.IDS_FIELD_NUMBER);
    }
  }

  public void validateOrThrow(RequestContext requestContext, UpdateWizIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateWizIntegrationRequest.ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, UpdateWizIntegrationRequest.NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, UpdateWizIntegrationRequest.CLIENT_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(request, UpdateWizIntegrationRequest.TOKEN_URL_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        request, UpdateWizIntegrationRequest.API_ENDPOINT_URL_FIELD_NUMBER);
    if (request.hasClientSecret()) {
      validateEncryptedText(request.getClientSecret());
    }

    // check if id exists in the db
    boolean wizIntegrationConfigAlreadyExists =
        wizIntegrationConfigStore.getObject(requestContext, request.getId()).isPresent();
    if (!wizIntegrationConfigAlreadyExists) {
      throw Status.NOT_FOUND
          .withDescription(
              String.format(
                  "Attempting to update an object that does not exist for id {%s}",
                  request.getId()))
          .asRuntimeException();
    }
  }

  public void validateOrThrow(RequestContext requestContext, DeleteWizIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteWizIntegrationRequest.ID_FIELD_NUMBER);

    // check if id exists in the db
    boolean wizIntegrationConfigAlreadyExists =
        wizIntegrationConfigStore.getObject(requestContext, request.getId()).isPresent();
    if (!wizIntegrationConfigAlreadyExists) {
      throw Status.NOT_FOUND
          .withDescription(
              String.format(
                  "Attempting to delete an object that does not exist for id {%s}",
                  request.getId()))
          .asRuntimeException();
    }
  }

  public void validateEncryptedText(EncryptedText encryptedText) {
    validateNonDefaultPresenceOrThrow(encryptedText, EncryptedText.KEY_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(encryptedText, EncryptedText.VALUE_FIELD_NUMBER);
  }
}
