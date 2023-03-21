package ai.traceable.splunk.integration.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.splunk.integration.config.service.api.v1.CreateSplunkIntegrationRequest;
import ai.traceable.splunk.integration.config.service.api.v1.DeleteSplunkIntegrationRequest;
import ai.traceable.splunk.integration.config.service.api.v1.EncryptedText;
import ai.traceable.splunk.integration.config.service.api.v1.GetSplunkIntegrationsRequest;
import ai.traceable.splunk.integration.config.service.api.v1.GetSplunkIntegrationsResponse;
import ai.traceable.splunk.integration.config.service.api.v1.SplunkIntegration;
import ai.traceable.splunk.integration.config.service.api.v1.SplunkIntegrationConfigServiceGrpc;
import ai.traceable.splunk.integration.config.service.api.v1.SplunkIntegrationDetails;
import ai.traceable.splunk.integration.config.service.api.v1.SplunkIntegrationsFilter;
import ai.traceable.splunk.integration.config.service.api.v1.UpdateSplunkIntegrationRequest;
import ai.traceable.splunk.integration.config.service.store.SplunkIntegrationConfigStore;
import ai.traceable.splunk.integration.config.service.validation.SplunkIntegrationConfigRequestValidator;
import io.grpc.Status;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SplunkIntegrationConfigServiceImplTest {
  private SplunkIntegrationConfigServiceGrpc.SplunkIntegrationConfigServiceBlockingStub
      splunkIntegrationServiceBlockingStub;

  @Mock private ConfigChangeEventGenerator configChangeEventGenerator;

  @Test
  public void testCreateSplunkIntegration() {
    setupMocks(new MockGenericConfigService().mockUpsert());

    CreateSplunkIntegrationRequest expected =
        CreateSplunkIntegrationRequest.newBuilder()
            .setHttpEventCollectorUrl("http://splunk-hec.traceable.ai")
            .setName("splunk integration")
            .setDescription("Splunk integration")
            .setApiToken(
                EncryptedText.newBuilder().setKeyId("keyid").setValue("cypherText").build())
            .build();
    SplunkIntegration actual =
        splunkIntegrationServiceBlockingStub.createSplunkIntegration(expected).getIntegration();
    assertNotNull(actual.getId());
    assertEquals(expected.getName(), actual.getDetails().getName());
    assertEquals(expected.getDescription(), actual.getDetails().getDescription());
    assertEquals(
        expected.getHttpEventCollectorUrl(), actual.getDetails().getHttpEventCollectorUrl());
    assertEquals(expected.getApiToken(), actual.getDetails().getApiToken());
  }

  @Test
  public void testUpdateSplunkIntegration() {
    setupMocks(new MockGenericConfigService().mockUpsert().mockGet().mockGetAll());

    CreateSplunkIntegrationRequest expected =
        CreateSplunkIntegrationRequest.newBuilder()
            .setHttpEventCollectorUrl("http://splunk-hec.traceable.ai")
            .setName("splunk integration")
            .setDescription("Splunk integration")
            .setApiToken(
                EncryptedText.newBuilder().setKeyId("keyid").setValue("cypherText").build())
            .build();

    SplunkIntegration existing = createIntegration(expected);

    SplunkIntegration withUpdates =
        SplunkIntegration.newBuilder()
            .setId(existing.getId())
            .setDetails(
                SplunkIntegrationDetails.newBuilder()
                    .setName("update-name")
                    .setHttpEventCollectorUrl(existing.getDetails().getHttpEventCollectorUrl())
                    .setApiToken(existing.getDetails().getApiToken())
                    .setDescription(existing.getDetails().getDescription())
                    .build())
            .build();

    SplunkIntegration upserted =
        splunkIntegrationServiceBlockingStub
            .updateSplunkIntegration(
                UpdateSplunkIntegrationRequest.newBuilder()
                    .setId(existing.getId())
                    .setName("update-name")
                    .setHttpEventCollectorUrl(existing.getDetails().getHttpEventCollectorUrl())
                    .setApiToken(existing.getDetails().getApiToken())
                    .setDescription(existing.getDetails().getDescription())
                    .build())
            .getIntegration();
    assertEquals(
        1,
        splunkIntegrationServiceBlockingStub
            .getSplunkIntegrations(GetSplunkIntegrationsRequest.newBuilder().build())
            .getIntegrationsList()
            .size());

    assertEquals(withUpdates, upserted);
  }

  @Test
  public void testUpdateNonExistentSplunkIntegration() {
    setupMocks(new MockGenericConfigService().mockUpsert());

    CreateSplunkIntegrationRequest expected =
        CreateSplunkIntegrationRequest.newBuilder()
            .setHttpEventCollectorUrl("http://splunk-hec.traceable.ai")
            .setName("splunk integration")
            .setDescription("Splunk integration")
            .setApiToken(
                EncryptedText.newBuilder().setKeyId("keyid").setValue("cypherText").build())
            .build();

    SplunkIntegration existing = createIntegration(expected);

    assertThrows(
        Status.NOT_FOUND.asRuntimeException().getClass(),
        () ->
            splunkIntegrationServiceBlockingStub.updateSplunkIntegration(
                UpdateSplunkIntegrationRequest.newBuilder()
                    .setId("non existent id")
                    .setName("update-name")
                    .setHttpEventCollectorUrl(existing.getDetails().getHttpEventCollectorUrl())
                    .setApiToken(existing.getDetails().getApiToken())
                    .setDescription(existing.getDetails().getDescription())
                    .build()));
  }

  @Test
  public void testDeleteIntegration() {
    setupMocks(new MockGenericConfigService().mockDelete().mockGetAll().mockUpsert());

    CreateSplunkIntegrationRequest request1 =
        CreateSplunkIntegrationRequest.newBuilder()
            .setHttpEventCollectorUrl("http://splunk-hec.traceable.ai")
            .setName("splunk integration 1")
            .setDescription("Splunk integration")
            .setApiToken(
                EncryptedText.newBuilder().setKeyId("keyid").setValue("cypherText").build())
            .build();

    SplunkIntegration existing1 = createIntegration(request1);

    CreateSplunkIntegrationRequest request2 =
        CreateSplunkIntegrationRequest.newBuilder()
            .setHttpEventCollectorUrl("http://splunk-hec.traceable.ai")
            .setName("splunk integration 2")
            .setDescription("Splunk integration")
            .setApiToken(
                EncryptedText.newBuilder().setKeyId("keyid").setValue("cypherText").build())
            .build();
    SplunkIntegration existing2 = createIntegration(request2);

    splunkIntegrationServiceBlockingStub.deleteSplunkIntegration(
        DeleteSplunkIntegrationRequest.newBuilder().setId(existing1.getId()).build());
    List<SplunkIntegration> actual =
        splunkIntegrationServiceBlockingStub
            .getSplunkIntegrations(GetSplunkIntegrationsRequest.newBuilder().build())
            .getIntegrationsList();
    assertEquals(1, actual.size());
    assertFalse(
        actual.stream()
            .anyMatch(
                splunkIntegration ->
                    splunkIntegration
                        .getDetails()
                        .getName()
                        .equals(existing1.getDetails().getName())));
  }

  @Test
  public void testGetAllIntegrations() {
    setupMocks(new MockGenericConfigService().mockGetAll().mockUpsert());

    CreateSplunkIntegrationRequest request1 =
        CreateSplunkIntegrationRequest.newBuilder()
            .setHttpEventCollectorUrl("http://splunk-hec.traceable.ai")
            .setName("splunk integration 1")
            .setDescription("Splunk integration")
            .setApiToken(
                EncryptedText.newBuilder().setKeyId("keyid").setValue("cypherText").build())
            .build();

    SplunkIntegration existing1 = createIntegration(request1);

    CreateSplunkIntegrationRequest request2 =
        CreateSplunkIntegrationRequest.newBuilder()
            .setHttpEventCollectorUrl("http://splunk-hec.traceable.ai")
            .setName("splunk integration 2")
            .setDescription("Splunk integration")
            .setApiToken(
                EncryptedText.newBuilder().setKeyId("keyid").setValue("cypherText").build())
            .build();
    SplunkIntegration existing2 = createIntegration(request2);

    List<SplunkIntegration> actual =
        splunkIntegrationServiceBlockingStub
            .getSplunkIntegrations(GetSplunkIntegrationsRequest.newBuilder().build())
            .getIntegrationsList();
    assertEquals(2, actual.size());
  }

  @Test
  public void testGetAllIntegrationsWithFilter() {
    setupMocks(new MockGenericConfigService().mockGetAll().mockUpsert());

    CreateSplunkIntegrationRequest request1 =
        CreateSplunkIntegrationRequest.newBuilder()
            .setHttpEventCollectorUrl("http://splunk-hec.traceable.ai")
            .setName("splunk integration 1")
            .setDescription("Splunk integration")
            .setApiToken(
                EncryptedText.newBuilder().setKeyId("keyid").setValue("cypherText").build())
            .build();

    SplunkIntegration existing1 = createIntegration(request1);

    CreateSplunkIntegrationRequest request2 =
        CreateSplunkIntegrationRequest.newBuilder()
            .setHttpEventCollectorUrl("http://splunk-hec.traceable.ai")
            .setName("splunk integration 2")
            .setDescription("Splunk integration")
            .setApiToken(
                EncryptedText.newBuilder().setKeyId("keyid").setValue("cypherText").build())
            .build();
    SplunkIntegration existing2 = createIntegration(request2);

    GetSplunkIntegrationsResponse response =
        splunkIntegrationServiceBlockingStub.getSplunkIntegrations(
            GetSplunkIntegrationsRequest.newBuilder()
                .setFilter(SplunkIntegrationsFilter.newBuilder().addIds(existing2.getId()).build())
                .build());
    assertEquals(1, response.getIntegrationsCount());

    SplunkIntegration result = response.getIntegrationsList().get(0);
    assertEquals(existing2.getId(), result.getId());
  }

  private SplunkIntegration createIntegration(CreateSplunkIntegrationRequest request) {
    return splunkIntegrationServiceBlockingStub.createSplunkIntegration(request).getIntegration();
  }

  private void setupMocks(MockGenericConfigService mockGenericConfigService) {
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
    SplunkIntegrationConfigStore splunkIntegrationConfigStore =
        new SplunkIntegrationConfigStore(genericStub, configChangeEventGenerator);

    mockGenericConfigService
        .addService(
            new SplunkIntegrationConfigServiceImpl(
                new SplunkIntegrationConfigRequestValidator(splunkIntegrationConfigStore),
                splunkIntegrationConfigStore,
                new UuidGenerator()))
        .start();

    this.splunkIntegrationServiceBlockingStub =
        SplunkIntegrationConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
  }
}
