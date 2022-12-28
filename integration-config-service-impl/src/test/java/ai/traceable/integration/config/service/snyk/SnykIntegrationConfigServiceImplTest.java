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
    assertThrows(
        RuntimeException.class,
        () ->
            snykIntegrationServiceBlockingStub.getSnykIntegrationDetails(
                GetSnykIntegrationDetailsRequest.getDefaultInstance()));
    assertThrows(
        RuntimeException.class,
        () ->
            snykIntegrationServiceBlockingStub.getSnykIntegrationSummary(
                GetSnykIntegrationSummaryRequest.getDefaultInstance()));
    assertThrows(
        RuntimeException.class,
        () ->
            snykIntegrationServiceBlockingStub.updateSnykIntegration(
                UpdateSnykIntegrationRequest.getDefaultInstance()));
    assertEquals(
        CreateSnykIntegrationResponse.getDefaultInstance(),
        snykIntegrationServiceBlockingStub.createSnykIntegration(
            CreateSnykIntegrationRequest.newBuilder()
                .setApiToken(EncryptedText.newBuilder().setKeyId("keyId").setValue("value").build())
                .build()));
    assertEquals(
        GetSnykIntegrationDetailsResponse.newBuilder()
            .setSnykIntegration(
                SnykIntegration.newBuilder()
                    .setApiToken(
                        EncryptedText.newBuilder().setKeyId("keyId").setValue("value").build())
                    .build())
            .build(),
        snykIntegrationServiceBlockingStub.getSnykIntegrationDetails(
            GetSnykIntegrationDetailsRequest.getDefaultInstance()));
    assertEquals(
        GetSnykIntegrationSummaryResponse.getDefaultInstance(),
        snykIntegrationServiceBlockingStub.getSnykIntegrationSummary(
            GetSnykIntegrationSummaryRequest.getDefaultInstance()));
    assertEquals(
        UpdateSnykIntegrationResponse.newBuilder().build(),
        snykIntegrationServiceBlockingStub.updateSnykIntegration(
            UpdateSnykIntegrationRequest.newBuilder()
                .setApiToken(
                    EncryptedText.newBuilder().setKeyId("keyId1").setValue("value").build())
                .build()));
    assertEquals(
        GetSnykIntegrationDetailsResponse.newBuilder()
            .setSnykIntegration(
                SnykIntegration.newBuilder()
                    .setApiToken(
                        EncryptedText.newBuilder().setKeyId("keyId1").setValue("value").build())
                    .build())
            .build(),
        snykIntegrationServiceBlockingStub.getSnykIntegrationDetails(
            GetSnykIntegrationDetailsRequest.getDefaultInstance()));
    assertEquals(
        GetSnykIntegrationSummaryResponse.getDefaultInstance(),
        snykIntegrationServiceBlockingStub.getSnykIntegrationSummary(
            GetSnykIntegrationSummaryRequest.getDefaultInstance()));
    assertEquals(
        DeleteSnykIntegrationResponse.getDefaultInstance(),
        snykIntegrationServiceBlockingStub.deleteSnykIntegration(
            DeleteSnykIntegrationRequest.getDefaultInstance()));
    assertThrows(
        RuntimeException.class,
        () ->
            snykIntegrationServiceBlockingStub.getSnykIntegrationDetails(
                GetSnykIntegrationDetailsRequest.getDefaultInstance()));
    assertThrows(
        RuntimeException.class,
        () ->
            snykIntegrationServiceBlockingStub.getSnykIntegrationSummary(
                GetSnykIntegrationSummaryRequest.getDefaultInstance()));
  }
}
