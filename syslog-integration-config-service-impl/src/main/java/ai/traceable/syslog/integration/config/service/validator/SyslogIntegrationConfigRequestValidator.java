package ai.traceable.syslog.integration.config.service.validator;

import static ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegrationDetails.LOG_FORMAT_FIELD_NUMBER;
import static ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegrationDetails.NAME_FIELD_NUMBER;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.syslog.integration.config.service.api.v1.CreateSyslogServerIntegrationRequest;
import ai.traceable.syslog.integration.config.service.api.v1.DeleteSyslogServerIntegrationRequest;
import ai.traceable.syslog.integration.config.service.api.v1.GetSyslogServerIntegrationsRequest;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogLogFormat;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerConnectionDetails;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerCredentials;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerCredentials.SyslogServerAccountTokenDetails;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegration;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegrationDetails;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegrationsFilter;
import ai.traceable.syslog.integration.config.service.api.v1.UpdateSyslogServerIntegrationRequest;
import ai.traceable.syslog.integration.config.service.store.SyslogIntegrationConfigStore;
import com.google.inject.Inject;
import io.grpc.Status;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class SyslogIntegrationConfigRequestValidator {
  private final SyslogIntegrationConfigStore syslogIntegrationConfigStore;

  public void validateOrThrow(
      RequestContext requestContext, CreateSyslogServerIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (!request.hasIntegrationDetails()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Integration details should be present to create syslog server integration")
          .asRuntimeException();
    }
    validateCreateIntegrationDetails(request.getIntegrationDetails());
  }

  public void validateOrThrow(
      RequestContext requestContext, GetSyslogServerIntegrationsRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (request.hasFilter()) {
      validateNonDefaultPresenceOrThrow(
          request.getFilter(), SyslogServerIntegrationsFilter.IDS_FIELD_NUMBER);
    }
  }

  public void validateOrThrow(
      RequestContext requestContext, UpdateSyslogServerIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
    if (!request.hasIntegration()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Integration should be present to update syslog integration")
          .asRuntimeException();
    }
    SyslogServerIntegration integration = request.getIntegration();
    validateNonDefaultPresenceOrThrow(integration, SyslogServerIntegration.ID_FIELD_NUMBER);
    if (!integration.hasDetails()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Integration details should be present to update syslog integration")
          .asRuntimeException();
    }
    validateUpdateIntegrationDetails(integration.getDetails());
    boolean exists =
        syslogIntegrationConfigStore.getObject(requestContext, integration.getId()).isPresent();
    if (!exists) {
      throw Status.NOT_FOUND
          .withDescription(
              String.format(
                  "Attempting to update an object that does not exist for id {%s}",
                  integration.getId()))
          .asRuntimeException();
    }
  }

  public void validateOrThrow(
      RequestContext requestContext, DeleteSyslogServerIntegrationRequest request) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(
        request, DeleteSyslogServerIntegrationRequest.ID_FIELD_NUMBER);
  }

  private void validateCreateIntegrationDetails(SyslogServerIntegrationDetails integrationDetails) {
    if (!integrationDetails.hasServerConnectionDetails()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Server Connection Details should be present to create syslog server integration")
          .asRuntimeException();
    }
    validateNonDefaultPresenceOrThrow(integrationDetails, NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(integrationDetails, LOG_FORMAT_FIELD_NUMBER);
    validateServerConnectionDetails(
        integrationDetails.getServerConnectionDetails(), integrationDetails.getLogFormat());
  }

  private void validateUpdateIntegrationDetails(SyslogServerIntegrationDetails integrationDetails) {
    validateNonDefaultPresenceOrThrow(integrationDetails, NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(integrationDetails, LOG_FORMAT_FIELD_NUMBER);
    validateServerConnectionDetails(
        integrationDetails.getServerConnectionDetails(), integrationDetails.getLogFormat());
  }

  private void validateServerConnectionDetails(
      SyslogServerConnectionDetails serverConnectionDetails, SyslogLogFormat logFormat) {
    validateNonDefaultPresenceOrThrow(
        serverConnectionDetails, SyslogServerConnectionDetails.HOST_FIELD_NUMBER);
    if (serverConnectionDetails.getPort() < 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Server port should be a positive number"))
          .asRuntimeException();
    }
    if (serverConnectionDetails.hasCredentials()) {
      validateSyslogServerCredentials(serverConnectionDetails.getCredentials(), logFormat);
    }
  }

  private void validateSyslogServerCredentials(
      SyslogServerCredentials credentials, SyslogLogFormat logFormat) {
    if (!SyslogServerCredentials.getDefaultInstance().equals(credentials)) {
      validateNonDefaultPresenceOrThrow(
          credentials, SyslogServerCredentials.ENCRYPTION_KEY_ID_FIELD_NUMBER);
      if (credentials.hasEncryptedSslCaCert()) {
        validateNonDefaultPresenceOrThrow(
            credentials, SyslogServerCredentials.ENCRYPTED_SSL_CA_CERT_FIELD_NUMBER);
      }
      if (credentials.hasAccountTokenDetails()) {
        validateSyslogServerAccountTokenDetails(credentials.getAccountTokenDetails(), logFormat);
      }
    }
  }

  private void validateSyslogServerAccountTokenDetails(
      SyslogServerAccountTokenDetails accountTokenDetails, SyslogLogFormat logFormat) {
    validateNonDefaultPresenceOrThrow(
        accountTokenDetails, SyslogServerAccountTokenDetails.ENCRYPTED_TOKEN_ID_FIELD_NUMBER);
    if (accountTokenDetails.getPrivateIdentificationNumber() <= 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format("Private Identification Number should be a positive number"))
          .asRuntimeException();
    }
    if (!logFormat.equals(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_5424)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Account Token Details require Syslog format to be RFC_5424")
          .asRuntimeException();
    }
  }
}
