package ai.traceable.syslog.integration.config.service.validator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.syslog.integration.config.service.api.v1.CreateSyslogServerIntegrationRequest;
import ai.traceable.syslog.integration.config.service.api.v1.DeleteSyslogServerIntegrationRequest;
import ai.traceable.syslog.integration.config.service.api.v1.GetSyslogServerIntegrationsRequest;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogLogFormat;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerConnectionDetails;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerCredentials;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegration;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegrationDetails;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegrationsFilter;
import ai.traceable.syslog.integration.config.service.api.v1.UpdateSyslogServerIntegrationRequest;
import ai.traceable.syslog.integration.config.service.store.SyslogIntegrationConfigStore;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class SyslogIntegrationConfigRequestValidatorTest {
  private RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("test-tenant");
  private SyslogIntegrationConfigStore syslogIntegrationConfigStore =
      mock(SyslogIntegrationConfigStore.class);
  private SyslogIntegrationConfigRequestValidator syslogIntegrationConfigRequestValidator =
      new SyslogIntegrationConfigRequestValidator(syslogIntegrationConfigStore);

  @Test
  void testGetRequestValidation() {
    GetSyslogServerIntegrationsRequest request1 =
        GetSyslogServerIntegrationsRequest.newBuilder()
            .setFilter(SyslogServerIntegrationsFilter.newBuilder().addAllIds(List.of()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request1));

    GetSyslogServerIntegrationsRequest request2 =
        GetSyslogServerIntegrationsRequest.newBuilder()
            .setFilter(SyslogServerIntegrationsFilter.newBuilder().addAllIds(List.of("id1")))
            .build();
    assertDoesNotThrow(
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request2));
  }

  @Test
  void testDeleteRequestValidation() {
    DeleteSyslogServerIntegrationRequest request1 =
        DeleteSyslogServerIntegrationRequest.getDefaultInstance();
    assertThrows(
        StatusRuntimeException.class,
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request1));

    DeleteSyslogServerIntegrationRequest request2 =
        DeleteSyslogServerIntegrationRequest.newBuilder().setId("id1").build();
    assertDoesNotThrow(
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request2));
  }

  @Test
  void testCreateRequestValidation() {

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT, CreateSyslogServerIntegrationRequest.getDefaultInstance()));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getCreateRequest(
                    "",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.getDefaultInstance())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getCreateRequest(
                    "name",
                    "",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.getDefaultInstance())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getCreateRequest(
                    "name",
                    "host",
                    -1,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.getDefaultInstance())));

    assertDoesNotThrow(
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getCreateRequest(
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.getDefaultInstance())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getCreateRequest(
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("keyId")
                        .setEncryptedSslCaCert("")
                        .build())));

    assertDoesNotThrow(
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getCreateRequest(
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("keyId")
                        .setEncryptedSslCaCert("cert")
                        .build())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getCreateRequest(
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_5424,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("keyId")
                        .setAccountTokenDetails(
                            SyslogServerCredentials.SyslogServerAccountTokenDetails.newBuilder()
                                .setEncryptedTokenId("id")
                                .setPrivateIdentificationNumber(0))
                        .build())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getCreateRequest(
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_5424,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("keyId")
                        .setAccountTokenDetails(
                            SyslogServerCredentials.SyslogServerAccountTokenDetails.newBuilder()
                                .setEncryptedTokenId("")
                                .setPrivateIdentificationNumber(12345))
                        .build())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getCreateRequest(
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_5424,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("")
                        .setAccountTokenDetails(
                            SyslogServerCredentials.SyslogServerAccountTokenDetails.newBuilder()
                                .setEncryptedTokenId("id")
                                .setPrivateIdentificationNumber(12345))
                        .build())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getCreateRequest(
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("id")
                        .setAccountTokenDetails(
                            SyslogServerCredentials.SyslogServerAccountTokenDetails.newBuilder()
                                .setEncryptedTokenId("id")
                                .setPrivateIdentificationNumber(12345))
                        .build())));

    assertDoesNotThrow(
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getCreateRequest(
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_5424,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("id")
                        .setAccountTokenDetails(
                            SyslogServerCredentials.SyslogServerAccountTokenDetails.newBuilder()
                                .setEncryptedTokenId("id")
                                .setPrivateIdentificationNumber(12345))
                        .build())));
  }

  @Test
  void testUpdateRequestValidation() {
    when(syslogIntegrationConfigStore.getObject(any(), eq("id")))
        .thenReturn(
            Optional.of(
                new SampleContextualConfigObject<>(SyslogServerIntegration.getDefaultInstance())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT, UpdateSyslogServerIntegrationRequest.getDefaultInstance()));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getUpdateRequest(
                    "",
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.getDefaultInstance())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getUpdateRequest(
                    "id",
                    "",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.getDefaultInstance())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getUpdateRequest(
                    "id",
                    "name",
                    "",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.getDefaultInstance())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getUpdateRequest(
                    "id",
                    "name",
                    "host",
                    -1,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.getDefaultInstance())));

    assertDoesNotThrow(
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getUpdateRequest(
                    "id",
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.getDefaultInstance())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getUpdateRequest(
                    "id",
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("keyId")
                        .setEncryptedSslCaCert("")
                        .build())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getUpdateRequest(
                    "id-not-existing",
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("keyId")
                        .setEncryptedSslCaCert("cert")
                        .build())));

    assertDoesNotThrow(
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getUpdateRequest(
                    "id",
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("keyId")
                        .setEncryptedSslCaCert("cert")
                        .build())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getUpdateRequest(
                    "id",
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_5424,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("keyId")
                        .setAccountTokenDetails(
                            SyslogServerCredentials.SyslogServerAccountTokenDetails.newBuilder()
                                .setEncryptedTokenId("id")
                                .setPrivateIdentificationNumber(0))
                        .build())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getUpdateRequest(
                    "id",
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_5424,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("keyId")
                        .setAccountTokenDetails(
                            SyslogServerCredentials.SyslogServerAccountTokenDetails.newBuilder()
                                .setEncryptedTokenId("")
                                .setPrivateIdentificationNumber(12345))
                        .build())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getUpdateRequest(
                    "id",
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_5424,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("")
                        .setAccountTokenDetails(
                            SyslogServerCredentials.SyslogServerAccountTokenDetails.newBuilder()
                                .setEncryptedTokenId("id")
                                .setPrivateIdentificationNumber(12345))
                        .build())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getUpdateRequest(
                    "id",
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("id")
                        .setAccountTokenDetails(
                            SyslogServerCredentials.SyslogServerAccountTokenDetails.newBuilder()
                                .setEncryptedTokenId("id")
                                .setPrivateIdentificationNumber(12345))
                        .build())));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getUpdateRequest(
                    "id-not-existing",
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_5424,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("id")
                        .setAccountTokenDetails(
                            SyslogServerCredentials.SyslogServerAccountTokenDetails.newBuilder()
                                .setEncryptedTokenId("id")
                                .setPrivateIdentificationNumber(12345))
                        .build())));

    assertDoesNotThrow(
        () ->
            syslogIntegrationConfigRequestValidator.validateOrThrow(
                REQUEST_CONTEXT,
                getUpdateRequest(
                    "id",
                    "name",
                    "host",
                    0,
                    SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_5424,
                    SyslogServerCredentials.newBuilder()
                        .setEncryptionKeyId("id")
                        .setAccountTokenDetails(
                            SyslogServerCredentials.SyslogServerAccountTokenDetails.newBuilder()
                                .setEncryptedTokenId("id")
                                .setPrivateIdentificationNumber(12345))
                        .build())));
  }

  private CreateSyslogServerIntegrationRequest getCreateRequest(
      String name,
      String host,
      int port,
      SyslogLogFormat logFormat,
      SyslogServerCredentials credentials) {
    return CreateSyslogServerIntegrationRequest.newBuilder()
        .setIntegrationDetails(
            getSyslogServerIntegrationDetails(name, host, port, logFormat, credentials))
        .build();
  }

  private UpdateSyslogServerIntegrationRequest getUpdateRequest(
      String id,
      String name,
      String host,
      int port,
      SyslogLogFormat logFormat,
      SyslogServerCredentials credentials) {
    return UpdateSyslogServerIntegrationRequest.newBuilder()
        .setIntegration(
            SyslogServerIntegration.newBuilder()
                .setId(id)
                .setDetails(
                    getSyslogServerIntegrationDetails(name, host, port, logFormat, credentials)))
        .build();
  }

  private SyslogServerIntegrationDetails getSyslogServerIntegrationDetails(
      String name,
      String host,
      int port,
      SyslogLogFormat logFormat,
      SyslogServerCredentials credentials) {
    return SyslogServerIntegrationDetails.newBuilder()
        .setName(name)
        .setLogFormat(logFormat)
        .setServerConnectionDetails(
            SyslogServerConnectionDetails.newBuilder()
                .setHost(host)
                .setPort(port)
                .setCredentials(credentials))
        .build();
  }

  private static class SampleContextualConfigObject<T> implements ContextualConfigObject<T> {

    private final T data;
    private final String context;
    private final Instant creationTimestamp;
    private final Instant lastUpdatedTimestamp;

    SampleContextualConfigObject(T data) {
      this.data = data;
      this.context = "context";
      this.creationTimestamp = Instant.now();
      this.lastUpdatedTimestamp = Instant.now();
    }

    @Override
    public T getData() {
      return data;
    }

    @Override
    public Instant getCreationTimestamp() {
      return creationTimestamp;
    }

    @Override
    public Instant getLastUpdatedTimestamp() {
      return lastUpdatedTimestamp;
    }

    @Override
    public String getContext() {
      return context;
    }
  }
}
