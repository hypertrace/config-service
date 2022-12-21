package ai.traceable.integration.config.service.snyk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.integration.config.service.snyk.store.SnykIntegrationConfigStore;
import ai.traceable.integration.config.service.snyk.v1.CreateSnykIntegrationRequest;
import ai.traceable.integration.config.service.snyk.v1.CreateSnykIntegrationResponse;
import ai.traceable.integration.config.service.snyk.v1.DeleteSnykIntegrationRequest;
import ai.traceable.integration.config.service.snyk.v1.DeleteSnykIntegrationResponse;
import ai.traceable.integration.config.service.snyk.v1.EncryptedText;
import ai.traceable.integration.config.service.snyk.v1.GetSnykIntegrationDetailsRequest;
import ai.traceable.integration.config.service.snyk.v1.GetSnykIntegrationDetailsResponse;
import ai.traceable.integration.config.service.snyk.v1.GetSnykIntegrationSummaryRequest;
import ai.traceable.integration.config.service.snyk.v1.GetSnykIntegrationSummaryResponse;
import ai.traceable.integration.config.service.snyk.v1.SnykIntegration;
import ai.traceable.integration.config.service.snyk.v1.SnykIntegrationServiceGrpc;
import ai.traceable.integration.config.service.snyk.v1.SnykIntegrationSummary;
import ai.traceable.integration.config.service.snyk.v1.UpdateSnykIntegrationRequest;
import ai.traceable.integration.config.service.snyk.v1.UpdateSnykIntegrationResponse;
import ai.traceable.integration.config.service.snyk.validation.SnykIntegrationConfigRequestValidator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnykIntegrationConfigServiceImplTest {

  private SnykIntegrationServiceGrpc.SnykIntegrationServiceBlockingStub
      snykIntegrationServiceBlockingStub;

  @BeforeEach
  void beforeEach() {
    MockGenericConfigService mockGenericConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockUpsertAll();

    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());

    SnykIntegrationConfigStore snykIntegrationConfigStore =
        new SnykIntegrationConfigStore(genericStub);

    mockGenericConfigService
        .addService(
            new SnykIntegrationConfigServiceImpl(
                new SnykIntegrationConfigRequestValidator(), snykIntegrationConfigStore))
        .start();

    this.snykIntegrationServiceBlockingStub =
        SnykIntegrationServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
  }

  @Test
  void testSnykIntegrationConfigCrud() {
    assertEquals(
        GetSnykIntegrationDetailsResponse.getDefaultInstance(),
        snykIntegrationServiceBlockingStub.getSnykIntegrationDetails(
            GetSnykIntegrationDetailsRequest.getDefaultInstance()));
    assertEquals(
        GetSnykIntegrationSummaryResponse.getDefaultInstance(),
        snykIntegrationServiceBlockingStub.getSnykIntegrationSummary(
            GetSnykIntegrationSummaryRequest.getDefaultInstance()));
    assertThrows(
        RuntimeException.class,
        () ->
            snykIntegrationServiceBlockingStub.updateSnykIntegration(
                UpdateSnykIntegrationRequest.newBuilder().setName("name").build()));
    assertEquals(
        CreateSnykIntegrationResponse.newBuilder()
            .setSnykIntegrationSummary(SnykIntegrationSummary.newBuilder().setName("name").build())
            .build(),
        snykIntegrationServiceBlockingStub.createSnykIntegration(
            CreateSnykIntegrationRequest.newBuilder()
                .setName("name")
                .setApiToken(EncryptedText.newBuilder().setKeyId("keyId").setValue("value").build())
                .build()));
    assertEquals(
        GetSnykIntegrationDetailsResponse.newBuilder()
            .setSnykIntegration(
                SnykIntegration.newBuilder()
                    .setSnykIntegrationSummary(
                        SnykIntegrationSummary.newBuilder().setName("name").build())
                    .setApiToken(
                        EncryptedText.newBuilder().setKeyId("keyId").setValue("value").build())
                    .build())
            .build(),
        snykIntegrationServiceBlockingStub.getSnykIntegrationDetails(
            GetSnykIntegrationDetailsRequest.getDefaultInstance()));
    assertEquals(
        GetSnykIntegrationSummaryResponse.newBuilder()
            .setSnykIntegrationSummary(SnykIntegrationSummary.newBuilder().setName("name").build())
            .build(),
        snykIntegrationServiceBlockingStub.getSnykIntegrationSummary(
            GetSnykIntegrationSummaryRequest.getDefaultInstance()));
    assertEquals(
        UpdateSnykIntegrationResponse.newBuilder()
            .setSnykIntegrationSummary(SnykIntegrationSummary.newBuilder().setName("name1").build())
            .build(),
        snykIntegrationServiceBlockingStub.updateSnykIntegration(
            UpdateSnykIntegrationRequest.newBuilder()
                .setName("name1")
                .setApiToken(
                    EncryptedText.newBuilder().setKeyId("keyId1").setValue("value").build())
                .build()));
    assertEquals(
        GetSnykIntegrationDetailsResponse.newBuilder()
            .setSnykIntegration(
                SnykIntegration.newBuilder()
                    .setSnykIntegrationSummary(
                        SnykIntegrationSummary.newBuilder().setName("name1").build())
                    .setApiToken(
                        EncryptedText.newBuilder().setKeyId("keyId1").setValue("value").build())
                    .build())
            .build(),
        snykIntegrationServiceBlockingStub.getSnykIntegrationDetails(
            GetSnykIntegrationDetailsRequest.getDefaultInstance()));
    assertEquals(
        GetSnykIntegrationSummaryResponse.newBuilder()
            .setSnykIntegrationSummary(SnykIntegrationSummary.newBuilder().setName("name1").build())
            .build(),
        snykIntegrationServiceBlockingStub.getSnykIntegrationSummary(
            GetSnykIntegrationSummaryRequest.getDefaultInstance()));
    assertEquals(
        DeleteSnykIntegrationResponse.getDefaultInstance(),
        snykIntegrationServiceBlockingStub.deleteSnykIntegration(
            DeleteSnykIntegrationRequest.getDefaultInstance()));
    assertEquals(
        GetSnykIntegrationDetailsResponse.getDefaultInstance(),
        snykIntegrationServiceBlockingStub.getSnykIntegrationDetails(
            GetSnykIntegrationDetailsRequest.getDefaultInstance()));
    assertEquals(
        GetSnykIntegrationSummaryResponse.getDefaultInstance(),
        snykIntegrationServiceBlockingStub.getSnykIntegrationSummary(
            GetSnykIntegrationSummaryRequest.getDefaultInstance()));
  }
}
