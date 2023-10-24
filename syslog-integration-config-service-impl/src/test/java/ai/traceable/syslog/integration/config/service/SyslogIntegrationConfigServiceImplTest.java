package ai.traceable.syslog.integration.config.service;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.syslog.integration.config.service.api.v1.CreateSyslogServerIntegrationRequest;
import ai.traceable.syslog.integration.config.service.api.v1.DeleteSyslogServerIntegrationRequest;
import ai.traceable.syslog.integration.config.service.api.v1.GetSyslogServerIntegrationsRequest;
import ai.traceable.syslog.integration.config.service.api.v1.GetSyslogServerIntegrationsResponse;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogIntegrationConfigServiceGrpc;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogLogFormat;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerConnectionDetails;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegration;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegrationDetails;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerIntegrationsFilter;
import ai.traceable.syslog.integration.config.service.api.v1.SyslogServerSslCredentials;
import ai.traceable.syslog.integration.config.service.api.v1.UpdateSyslogServerIntegrationRequest;
import ai.traceable.syslog.integration.config.service.store.SyslogIntegrationConfigStore;
import ai.traceable.syslog.integration.config.service.validator.SyslogIntegrationConfigRequestValidator;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SyslogIntegrationConfigServiceImplTest {

  private SyslogIntegrationConfigServiceGrpc.SyslogIntegrationConfigServiceBlockingStub
      syslogIntegrationConfigServiceBlockingStub;
  @Mock private ConfigChangeEventGenerator configChangeEventGenerator;

  private SyslogServerIntegration createIntegration(CreateSyslogServerIntegrationRequest request) {
    return syslogIntegrationConfigServiceBlockingStub
        .createSyslogServerIntegration(request)
        .getIntegration();
  }

  @Test
  public void testCreateSyslogIntegration() {
    setupMocks(new MockGenericConfigService().mockUpsert());

    CreateSyslogServerIntegrationRequest expected =
        CreateSyslogServerIntegrationRequest.newBuilder()
            .setIntegrationDetails(
                SyslogServerIntegrationDetails.newBuilder()
                    .setName("syslog integration")
                    .setDescription("Syslog integration")
                    .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164)
                    .setServerConnectionDetails(
                        SyslogServerConnectionDetails.newBuilder()
                            .setHost("host")
                            .setPort(1001)
                            .setSslCredentials(SyslogServerSslCredentials.newBuilder())))
            .build();
    SyslogServerIntegration actual =
        syslogIntegrationConfigServiceBlockingStub
            .createSyslogServerIntegration(expected)
            .getIntegration();
    assertNotNull(actual.getId());
    assertEquals(expected.getIntegrationDetails().getName(), actual.getDetails().getName());
    assertEquals(
        expected.getIntegrationDetails().getDescription(), actual.getDetails().getDescription());
    assertEquals(
        expected.getIntegrationDetails().getLogFormat(), actual.getDetails().getLogFormat());
    assertEquals(
        expected.getIntegrationDetails().getServerConnectionDetails(),
        actual.getDetails().getServerConnectionDetails());
  }

  @Test
  public void testUpdateSyslogIntegration() {
    setupMocks(new MockGenericConfigService().mockUpsert().mockGet().mockGetAll());

    CreateSyslogServerIntegrationRequest expected =
        CreateSyslogServerIntegrationRequest.newBuilder()
            .setIntegrationDetails(
                SyslogServerIntegrationDetails.newBuilder()
                    .setName("syslog integration")
                    .setDescription("Syslog integration")
                    .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164)
                    .setServerConnectionDetails(
                        SyslogServerConnectionDetails.newBuilder()
                            .setHost("host")
                            .setPort(1001)
                            .setSslCredentials(SyslogServerSslCredentials.newBuilder())))
            .build();

    SyslogServerIntegration existing = createIntegration(expected);

    SyslogServerIntegration withUpdates =
        SyslogServerIntegration.newBuilder()
            .setId(existing.getId())
            .setDetails(
                SyslogServerIntegrationDetails.newBuilder()
                    .setName("updatedName")
                    .setDescription(existing.getDetails().getDescription())
                    .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_5424)
                    .setServerConnectionDetails(
                        SyslogServerConnectionDetails.newBuilder()
                            .setHost("updatedHost")
                            .setPort(1002)
                            .setSslCredentials(SyslogServerSslCredentials.newBuilder()))
                    .build())
            .build();

    SyslogServerIntegration upserted =
        syslogIntegrationConfigServiceBlockingStub
            .updateSyslogServerIntegration(
                UpdateSyslogServerIntegrationRequest.newBuilder()
                    .setIntegration(withUpdates)
                    .build())
            .getIntegration();
    assertEquals(
        1,
        syslogIntegrationConfigServiceBlockingStub
            .getSyslogServerIntegrations(GetSyslogServerIntegrationsRequest.newBuilder().build())
            .getIntegrationsList()
            .size());

    assertEquals(withUpdates, upserted);
  }

  @Test
  public void testDeleteIntegration() {
    setupMocks(new MockGenericConfigService().mockDelete().mockGetAll().mockUpsert());

    CreateSyslogServerIntegrationRequest request1 =
        CreateSyslogServerIntegrationRequest.newBuilder()
            .setIntegrationDetails(
                SyslogServerIntegrationDetails.newBuilder()
                    .setName("syslog integration 1")
                    .setDescription("Syslog integration 1")
                    .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164)
                    .setServerConnectionDetails(
                        SyslogServerConnectionDetails.newBuilder()
                            .setHost("host")
                            .setPort(1001)
                            .setSslCredentials(SyslogServerSslCredentials.newBuilder())))
            .build();

    SyslogServerIntegration existing1 = createIntegration(request1);

    CreateSyslogServerIntegrationRequest request2 =
        CreateSyslogServerIntegrationRequest.newBuilder()
            .setIntegrationDetails(
                SyslogServerIntegrationDetails.newBuilder()
                    .setName("syslog integration 2")
                    .setDescription("Syslog integration 2")
                    .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164)
                    .setServerConnectionDetails(
                        SyslogServerConnectionDetails.newBuilder()
                            .setHost("host")
                            .setPort(1001)
                            .setSslCredentials(SyslogServerSslCredentials.newBuilder())))
            .build();
    createIntegration(request2);

    syslogIntegrationConfigServiceBlockingStub.deleteSyslogServerIntegration(
        DeleteSyslogServerIntegrationRequest.newBuilder().setId(existing1.getId()).build());
    List<SyslogServerIntegration> actual =
        syslogIntegrationConfigServiceBlockingStub
            .getSyslogServerIntegrations(GetSyslogServerIntegrationsRequest.newBuilder().build())
            .getIntegrationsList();
    assertEquals(1, actual.size());
    assertFalse(
        actual.stream()
            .anyMatch(
                syslogServerIntegration ->
                    syslogServerIntegration
                        .getDetails()
                        .getName()
                        .equals(existing1.getDetails().getName())));
  }

  @Test
  public void testGetAllIntegrations() {
    setupMocks(new MockGenericConfigService().mockGetAll().mockUpsert());

    CreateSyslogServerIntegrationRequest request1 =
        CreateSyslogServerIntegrationRequest.newBuilder()
            .setIntegrationDetails(
                SyslogServerIntegrationDetails.newBuilder()
                    .setName("syslog integration 1")
                    .setDescription("Syslog integration 1")
                    .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164)
                    .setServerConnectionDetails(
                        SyslogServerConnectionDetails.newBuilder()
                            .setHost("host")
                            .setPort(1001)
                            .setSslCredentials(SyslogServerSslCredentials.newBuilder())))
            .build();

    createIntegration(request1);

    CreateSyslogServerIntegrationRequest request2 =
        CreateSyslogServerIntegrationRequest.newBuilder()
            .setIntegrationDetails(
                SyslogServerIntegrationDetails.newBuilder()
                    .setName("syslog integration 2")
                    .setDescription("Syslog integration 2")
                    .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164)
                    .setServerConnectionDetails(
                        SyslogServerConnectionDetails.newBuilder()
                            .setHost("host")
                            .setPort(1001)
                            .setSslCredentials(SyslogServerSslCredentials.newBuilder())))
            .build();

    createIntegration(request2);

    List<SyslogServerIntegration> actual =
        syslogIntegrationConfigServiceBlockingStub
            .getSyslogServerIntegrations(GetSyslogServerIntegrationsRequest.newBuilder().build())
            .getIntegrationsList();
    assertEquals(2, actual.size());
  }

  @Test
  public void testGetAllIntegrationsWithFilter() {
    setupMocks(new MockGenericConfigService().mockGetAll().mockUpsert());

    CreateSyslogServerIntegrationRequest request1 =
        CreateSyslogServerIntegrationRequest.newBuilder()
            .setIntegrationDetails(
                SyslogServerIntegrationDetails.newBuilder()
                    .setName("syslog integration 1")
                    .setDescription("Syslog integration 1")
                    .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164)
                    .setServerConnectionDetails(
                        SyslogServerConnectionDetails.newBuilder()
                            .setHost("host")
                            .setPort(1001)
                            .setSslCredentials(SyslogServerSslCredentials.newBuilder())))
            .build();

    createIntegration(request1);

    CreateSyslogServerIntegrationRequest request2 =
        CreateSyslogServerIntegrationRequest.newBuilder()
            .setIntegrationDetails(
                SyslogServerIntegrationDetails.newBuilder()
                    .setName("syslog integration 2")
                    .setDescription("Syslog integration 2")
                    .setLogFormat(SyslogLogFormat.SYSLOG_LOG_FORMAT_RFC_3164)
                    .setServerConnectionDetails(
                        SyslogServerConnectionDetails.newBuilder()
                            .setHost("host")
                            .setPort(1001)
                            .setSslCredentials(SyslogServerSslCredentials.newBuilder())))
            .build();
    SyslogServerIntegration existing = createIntegration(request2);

    GetSyslogServerIntegrationsResponse response =
        syslogIntegrationConfigServiceBlockingStub.getSyslogServerIntegrations(
            GetSyslogServerIntegrationsRequest.newBuilder()
                .setFilter(
                    SyslogServerIntegrationsFilter.newBuilder().addIds(existing.getId()).build())
                .build());
    assertEquals(1, response.getIntegrationsCount());

    SyslogServerIntegration result = response.getIntegrationsList().get(0);
    assertEquals(existing.getId(), result.getId());
  }

  private void setupMocks(MockGenericConfigService mockGenericConfigService) {
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
    SyslogIntegrationConfigStore syslogIntegrationConfigStore =
        new SyslogIntegrationConfigStore(genericStub, configChangeEventGenerator);

    mockGenericConfigService
        .addService(
            new SyslogIntegrationConfigServiceImpl(
                new SyslogIntegrationConfigRequestValidator(syslogIntegrationConfigStore),
                syslogIntegrationConfigStore,
                new UuidGenerator()))
        .start();

    this.syslogIntegrationConfigServiceBlockingStub =
        SyslogIntegrationConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
  }
}
