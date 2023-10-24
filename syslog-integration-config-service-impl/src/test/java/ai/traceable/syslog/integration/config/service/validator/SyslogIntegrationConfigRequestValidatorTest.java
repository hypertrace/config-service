package ai.traceable.syslog.integration.config.service.validator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import ai.traceable.syslog.integration.config.service.api.v1.CreateSyslogServerIntegrationRequest;
import ai.traceable.syslog.integration.config.service.api.v1.DeleteSyslogServerIntegrationRequest;
import ai.traceable.syslog.integration.config.service.api.v1.GetSyslogServerIntegrationsRequest;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogLogFormat;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerConnectionDetails;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegration;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegrationDetails;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegrationsFilter;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerSslCredentials;
import ai.traceable.syslog.integration.config.service.api.v1.UpdateSyslogServerIntegrationRequest;
import ai.traceable.syslog.integration.config.service.store.SyslogIntegrationConfigStore;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class SyslogIntegrationConfigRequestValidatorTest {
  private RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("test-tenant");
  private SyslogIntegrationConfigStore syslogIntegrationConfigStore =
      mock(SyslogIntegrationConfigStore.class);
  private SyslogIntegrationConfigRequestValidator syslogIntegrationConfigRequestValidator =
      new SyslogIntegrationConfigRequestValidator(syslogIntegrationConfigStore);

  @Test
  void testCreateRequestValidation() {
    CreateSyslogServerIntegrationRequest request1 =
        CreateSyslogServerIntegrationRequest.getDefaultInstance();
    assertThrows(
        StatusRuntimeException.class,
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request1));

    CreateSyslogServerIntegrationRequest request2 =
        CreateSyslogServerIntegrationRequest.newBuilder()
            .setIntegrationDetails(
                SyslogServerIntegrationDetails.newBuilder()
                    .setServerConnectionDetails(
                        SyslogServerConnectionDetails.newBuilder()
                            .setHost("host")
                            .setPort(1001)
                            .setSslCredentials(SyslogServerSslCredentials.newBuilder()))
                    .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request2));

    CreateSyslogServerIntegrationRequest request3 =
        CreateSyslogServerIntegrationRequest.newBuilder()
            .setIntegrationDetails(
                SyslogServerIntegrationDetails.newBuilder()
                    .setName("syslogIntegration")
                    .setServerConnectionDetails(
                        SyslogServerConnectionDetails.newBuilder()
                            .setPort(1001)
                            .setSslCredentials(SyslogServerSslCredentials.newBuilder()))
                    .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request3));

    CreateSyslogServerIntegrationRequest request4 =
        CreateSyslogServerIntegrationRequest.newBuilder()
            .setIntegrationDetails(
                SyslogServerIntegrationDetails.newBuilder()
                    .setName("syslogIntegration")
                    .setServerConnectionDetails(
                        SyslogServerConnectionDetails.newBuilder()
                            .setHost("host")
                            .setSslCredentials(SyslogServerSslCredentials.newBuilder()))
                    .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request4));

    CreateSyslogServerIntegrationRequest request5 =
        CreateSyslogServerIntegrationRequest.newBuilder()
            .setIntegrationDetails(
                SyslogServerIntegrationDetails.newBuilder()
                    .setName("syslogIntegration")
                    .setServerConnectionDetails(
                        SyslogServerConnectionDetails.newBuilder()
                            .setHost("host")
                            .setPort(1001)
                            .setSslCredentials(SyslogServerSslCredentials.newBuilder())))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request5));

    CreateSyslogServerIntegrationRequest request8 =
        CreateSyslogServerIntegrationRequest.newBuilder()
            .setIntegrationDetails(
                SyslogServerIntegrationDetails.newBuilder()
                    .setName("syslogIntegration")
                    .setServerConnectionDetails(
                        SyslogServerConnectionDetails.newBuilder()
                            .setHost("host")
                            .setPort(1001)
                            .setSslCredentials(SyslogServerSslCredentials.newBuilder()))
                    .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164))
            .build();
    assertDoesNotThrow(
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request8));
  }

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
  void testUpdateRequestValidation() {
    UpdateSyslogServerIntegrationRequest request1 =
        UpdateSyslogServerIntegrationRequest.getDefaultInstance();
    assertThrows(
        StatusRuntimeException.class,
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request1));

    UpdateSyslogServerIntegrationRequest request2 =
        UpdateSyslogServerIntegrationRequest.newBuilder()
            .setIntegration(
                SyslogServerIntegration.newBuilder()
                    .setId("id")
                    .setDetails(
                        SyslogServerIntegrationDetails.newBuilder()
                            .setServerConnectionDetails(
                                SyslogServerConnectionDetails.newBuilder()
                                    .setHost("host")
                                    .setPort(1001)
                                    .setSslCredentials(SyslogServerSslCredentials.newBuilder()))
                            .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164)))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request2));

    UpdateSyslogServerIntegrationRequest request3 =
        UpdateSyslogServerIntegrationRequest.newBuilder()
            .setIntegration(
                SyslogServerIntegration.newBuilder()
                    .setDetails(
                        SyslogServerIntegrationDetails.newBuilder()
                            .setName("syslogIntegration")
                            .setServerConnectionDetails(
                                SyslogServerConnectionDetails.newBuilder()
                                    .setHost("host")
                                    .setPort(1001)
                                    .setSslCredentials(SyslogServerSslCredentials.newBuilder()))
                            .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164)))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request3));

    UpdateSyslogServerIntegrationRequest request4 =
        UpdateSyslogServerIntegrationRequest.newBuilder()
            .setIntegration(SyslogServerIntegration.newBuilder().setId("id"))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request4));

    UpdateSyslogServerIntegrationRequest request5 =
        UpdateSyslogServerIntegrationRequest.newBuilder()
            .setIntegration(
                SyslogServerIntegration.newBuilder()
                    .setId("id")
                    .setDetails(
                        SyslogServerIntegrationDetails.newBuilder()
                            .setName("syslogIntegration")
                            .setServerConnectionDetails(
                                SyslogServerConnectionDetails.newBuilder()
                                    .setPort(1001)
                                    .setSslCredentials(SyslogServerSslCredentials.newBuilder()))
                            .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164)))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request5));

    UpdateSyslogServerIntegrationRequest request6 =
        UpdateSyslogServerIntegrationRequest.newBuilder()
            .setIntegration(
                SyslogServerIntegration.newBuilder()
                    .setId("id")
                    .setDetails(
                        SyslogServerIntegrationDetails.newBuilder()
                            .setName("syslogIntegration")
                            .setServerConnectionDetails(
                                SyslogServerConnectionDetails.newBuilder()
                                    .setHost("host")
                                    .setSslCredentials(SyslogServerSslCredentials.newBuilder()))
                            .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164)))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request6));

    UpdateSyslogServerIntegrationRequest request7 =
        UpdateSyslogServerIntegrationRequest.newBuilder()
            .setIntegration(
                SyslogServerIntegration.newBuilder()
                    .setId("id")
                    .setDetails(
                        SyslogServerIntegrationDetails.newBuilder()
                            .setName("syslogIntegration")
                            .setServerConnectionDetails(
                                SyslogServerConnectionDetails.newBuilder()
                                    .setHost("host")
                                    .setSslCredentials(SyslogServerSslCredentials.newBuilder()))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> syslogIntegrationConfigRequestValidator.validateOrThrow(REQUEST_CONTEXT, request7));
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
}
