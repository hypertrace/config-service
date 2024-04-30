package ai.traceable.integration.config.service.wiz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.integration.config.service.wiz.store.WizIntegrationConfigStore;
import ai.traceable.integration.config.service.wiz.v1.CreateWizIntegrationRequest;
import ai.traceable.integration.config.service.wiz.v1.DeleteWizIntegrationRequest;
import ai.traceable.integration.config.service.wiz.v1.EncryptedText;
import ai.traceable.integration.config.service.wiz.v1.GetWizIntegrationSummariesRequest;
import ai.traceable.integration.config.service.wiz.v1.GetWizIntegrationsRequest;
import ai.traceable.integration.config.service.wiz.v1.UpdateWizIntegrationRequest;
import ai.traceable.integration.config.service.wiz.v1.WizIntegration;
import ai.traceable.integration.config.service.wiz.v1.WizIntegrationConfigServiceGrpc;
import ai.traceable.integration.config.service.wiz.v1.WizIntegrationFilter;
import ai.traceable.integration.config.service.wiz.v1.WizIntegrationInfo;
import ai.traceable.integration.config.service.wiz.v1.WizIntegrationPreferences;
import ai.traceable.integration.config.service.wiz.v1.WizIntegrationSummary;
import ai.traceable.integration.config.service.wiz.v1.WizIssuePullConfiguration;
import ai.traceable.integration.config.service.wiz.validation.WizIntegrationConfigRequestValidator;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WizIntegrationConfigServiceImplTest {
  WizIntegrationConfigServiceGrpc.WizIntegrationConfigServiceBlockingStub
      wizIntegrationConfigServiceBlockingStub;

  @Mock private ConfigChangeEventGenerator configChangeEventGenerator;
  @Mock private WizIntegrationConfigRequestValidator wizIntegrationConfigRequestValidator;

  @Test
  void testCreateWizIntegration() {
    setupMocks(new MockGenericConfigService().mockUpsert());

    CreateWizIntegrationRequest expected =
        CreateWizIntegrationRequest.newBuilder()
            .setName("wiz integration")
            .setDescription("wiz integration for unit test")
            .setClientSecret(
                EncryptedText.newBuilder().setKeyId("keyid").setValue("cipher text").build())
            .setClientId("test customer")
            .setTokenUrl("http://getToken.wiz")
            .setApiEndpointUrl("http://us1-test.wiz")
            .build();

    WizIntegration actual =
        this.wizIntegrationConfigServiceBlockingStub
            .createWizIntegration(expected)
            .getIntegration();

    assertNotNull(actual.getId());
    assertEquals(expected.getName(), actual.getInfo().getName());
    assertEquals(expected.getDescription(), actual.getInfo().getDescription());
    assertEquals(expected.getClientSecret(), actual.getClientSecret());
    assertEquals(expected.getClientId(), actual.getInfo().getClientId());
    assertEquals(expected.getTokenUrl(), actual.getInfo().getTokenUrl());
    assertEquals(expected.getApiEndpointUrl(), actual.getInfo().getApiEndpointUrl());
  }

  @Test
  void testUpdateWizIntegration() {
    setupMocks(new MockGenericConfigService().mockUpsert().mockGetAll());

    WizIntegration existing =
        wizIntegrationConfigServiceBlockingStub
            .createWizIntegration(
                CreateWizIntegrationRequest.newBuilder()
                    .setName("wiz-1")
                    .setDescription("wiz integration for unit test")
                    .setClientSecret(
                        EncryptedText.newBuilder()
                            .setKeyId("keyid")
                            .setValue("cipher text")
                            .build())
                    .setClientId("test customer")
                    .setTokenUrl("http://getToken.wiz")
                    .setApiEndpointUrl("http://us1-test.wiz")
                    .build())
            .getIntegration();

    WizIntegration withUpdates =
        WizIntegration.newBuilder()
            .setId(existing.getId())
            .setClientSecret(existing.getClientSecret())
            .setInfo(
                WizIntegrationInfo.newBuilder()
                    .setName("updated Name")
                    .setDescription(existing.getInfo().getDescription())
                    .setClientId(existing.getInfo().getClientId())
                    .setApiEndpointUrl(existing.getInfo().getApiEndpointUrl())
                    .setTokenUrl(existing.getInfo().getTokenUrl())
                    .setWizIntegrationPreferences(
                        WizIntegrationPreferences.newBuilder()
                            .setWizIssuePullConfiguration(
                                WizIssuePullConfiguration.newBuilder().setEnabled(true)))
                    .build())
            .build();

    WizIntegration upserted =
        wizIntegrationConfigServiceBlockingStub
            .updateWizIntegration(
                UpdateWizIntegrationRequest.newBuilder()
                    .setId(existing.getId())
                    .setName("updated Name")
                    .setDescription(existing.getInfo().getDescription())
                    .setClientSecret(existing.getClientSecret())
                    .setClientId(existing.getInfo().getClientId())
                    .setTokenUrl(existing.getInfo().getTokenUrl())
                    .setApiEndpointUrl(existing.getInfo().getApiEndpointUrl())
                    .build())
            .getIntegration();
    assertEquals(
        1,
        wizIntegrationConfigServiceBlockingStub
            .getWizIntegrations(GetWizIntegrationsRequest.newBuilder().build())
            .getIntegrationsList()
            .size());
    assertEquals(withUpdates, upserted);
  }

  @Test
  public void testDeleteWizIntegration() {
    setupMocks(new MockGenericConfigService().mockUpsert().mockGetAll().mockDelete());

    WizIntegration existing1 =
        wizIntegrationConfigServiceBlockingStub
            .createWizIntegration(
                CreateWizIntegrationRequest.newBuilder()
                    .setName("wiz integration 1")
                    .setDescription("wiz integration for unit test")
                    .setClientSecret(
                        EncryptedText.newBuilder()
                            .setKeyId("keyid")
                            .setValue("cipher text")
                            .build())
                    .setClientId("test customer")
                    .setTokenUrl("http://getToken.wiz")
                    .setApiEndpointUrl("http://us1-test.wiz")
                    .build())
            .getIntegration();

    WizIntegration existing2 =
        wizIntegrationConfigServiceBlockingStub
            .createWizIntegration(
                CreateWizIntegrationRequest.newBuilder()
                    .setName("wiz integration 2")
                    .setDescription("wiz integration for unit test")
                    .setClientSecret(
                        EncryptedText.newBuilder()
                            .setKeyId("keyid")
                            .setValue("cipher text")
                            .build())
                    .setClientId("test customer")
                    .setTokenUrl("http://getToken.wiz")
                    .setApiEndpointUrl("http://us1-test.wiz")
                    .build())
            .getIntegration();

    wizIntegrationConfigServiceBlockingStub.deleteWizIntegration(
        DeleteWizIntegrationRequest.newBuilder().setId(existing1.getId()).build());

    List<WizIntegration> integrations =
        wizIntegrationConfigServiceBlockingStub
            .getWizIntegrations(GetWizIntegrationsRequest.newBuilder().build())
            .getIntegrationsList();

    assertEquals(1, integrations.size());
    assertFalse(
        integrations.stream()
            .anyMatch(
                wizIntegration ->
                    wizIntegration.getInfo().getName().equals(existing1.getInfo().getName())));
  }

  @Test
  void testGetWizIntegrationSummaries() {
    setupMocks(new MockGenericConfigService().mockUpsert().mockGetAll());

    WizIntegration wizIntegration =
        wizIntegrationConfigServiceBlockingStub
            .createWizIntegration(
                CreateWizIntegrationRequest.newBuilder()
                    .setName("wiz integration")
                    .setDescription("wiz integration for unit test")
                    .setClientSecret(
                        EncryptedText.newBuilder()
                            .setKeyId("keyid")
                            .setValue("cipher text")
                            .build())
                    .setClientId("test customer")
                    .setTokenUrl("http://getToken.wiz")
                    .setApiEndpointUrl("http://us1-test.wiz")
                    .build())
            .getIntegration();

    List<WizIntegrationSummary> summaries =
        wizIntegrationConfigServiceBlockingStub
            .getWizIntegrationSummaries(
                GetWizIntegrationSummariesRequest.newBuilder()
                    .setFilter(
                        WizIntegrationFilter.newBuilder().addIds(wizIntegration.getId()).build())
                    .build())
            .getIntegrationSummariesList();

    assertEquals(1, summaries.size());
    assertEquals(wizIntegration.getInfo().getName(), summaries.get(0).getInfo().getName());
  }

  @Test
  public void testGetWizIntegrationsWithFilter() {
    setupMocks(new MockGenericConfigService().mockUpsert().mockGetAll());

    WizIntegration integration1 =
        wizIntegrationConfigServiceBlockingStub
            .createWizIntegration(
                CreateWizIntegrationRequest.newBuilder()
                    .setName("wiz integration 1")
                    .setDescription("wiz integration for unit test")
                    .setClientSecret(
                        EncryptedText.newBuilder()
                            .setKeyId("keyid")
                            .setValue("cipher text")
                            .build())
                    .setClientId("test customer")
                    .setTokenUrl("http://getToken.wiz")
                    .setApiEndpointUrl("http://us1-test.wiz")
                    .build())
            .getIntegration();

    WizIntegration integration2 =
        wizIntegrationConfigServiceBlockingStub
            .createWizIntegration(
                CreateWizIntegrationRequest.newBuilder()
                    .setName("wiz integration 2")
                    .setDescription("wiz integration for unit test")
                    .setClientSecret(
                        EncryptedText.newBuilder()
                            .setKeyId("keyid")
                            .setValue("cipher text")
                            .build())
                    .setClientId("test customer")
                    .setTokenUrl("http://getToken.wiz")
                    .setApiEndpointUrl("http://us1-test.wiz")
                    .build())
            .getIntegration();

    WizIntegration integration3 =
        wizIntegrationConfigServiceBlockingStub
            .createWizIntegration(
                CreateWizIntegrationRequest.newBuilder()
                    .setName("wiz integration 3")
                    .setDescription("wiz integration for unit test")
                    .setClientSecret(
                        EncryptedText.newBuilder()
                            .setKeyId("keyid")
                            .setValue("cipher text")
                            .build())
                    .setClientId("test customer")
                    .setTokenUrl("http://getToken.wiz")
                    .setApiEndpointUrl("http://us1-test.wiz")
                    .build())
            .getIntegration();

    List<WizIntegration> integrations =
        wizIntegrationConfigServiceBlockingStub
            .getWizIntegrations(
                GetWizIntegrationsRequest.newBuilder()
                    .setFilter(
                        WizIntegrationFilter.newBuilder()
                            .addIds(integration1.getId())
                            .addIds(integration2.getId())
                            .build())
                    .build())
            .getIntegrationsList();

    assertEquals(2, integrations.size());
    assertTrue(
        integrations.stream()
            .anyMatch(
                wizIntegration ->
                    wizIntegration.getInfo().getName().equals(integration1.getInfo().getName())));
    assertFalse(
        integrations.stream()
            .anyMatch(
                wizIntegration ->
                    wizIntegration.getInfo().getName().equals(integration3.getInfo().getName())));
  }

  private void setupMocks(MockGenericConfigService mockGenericConfigService) {
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
    WizIntegrationConfigStore wizIntegrationConfigStore =
        new WizIntegrationConfigStore(genericStub, configChangeEventGenerator);

    mockGenericConfigService
        .addService(
            new WizIntegrationConfigServiceImpl(
                wizIntegrationConfigRequestValidator,
                wizIntegrationConfigStore,
                new UuidGenerator()))
        .start();

    this.wizIntegrationConfigServiceBlockingStub =
        WizIntegrationConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
  }
}
