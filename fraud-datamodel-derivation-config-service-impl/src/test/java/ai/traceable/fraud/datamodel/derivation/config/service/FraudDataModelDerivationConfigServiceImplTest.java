package ai.traceable.fraud.datamodel.derivation.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.datamodel.derivation.config.service.store.FraudDataModelDerivationConfigStore;
import ai.traceable.fraud.datamodel.derivation.config.service.store.FraudDataModelDerivationConfigStoreManager;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.CreateDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.DerivationConfig;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.DerivationConfigType;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.FraudDataModelDerivationConfigServiceGrpc;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigsRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.GetDerivationConfigsResponse;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.UpdateDerivationConfigRequest;
import ai.traceable.fraud.datamodel.derivation.config.service.validation.FraudDataModelDerivationConfigRequestValidator;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FraudDataModelDerivationConfigServiceImplTest {

  private static final String UUID_1 = "uuid-1";

  private FraudDataModelDerivationConfigServiceGrpc
          .FraudDataModelDerivationConfigServiceBlockingStub
      fraudDataModelDerivationConfigServiceBlockingStub;
  private FraudDataModelDerivationConfigStoreManager storeManager;
  private MockGenericConfigService mockGenericConfigService;
  @Mock private ConfigChangeEventGenerator eventGenerator;
  @Mock private UuidGenerator uuidGenerator;

  @BeforeEach
  void beforeEach() {
    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    this.storeManager =
        new FraudDataModelDerivationConfigStoreManager(
            new FraudDataModelDerivationConfigStore(genericStub, eventGenerator), uuidGenerator);
    this.mockGenericConfigService
        .addService(
            new FraudDataModelDerivationConfigServiceImpl(
                storeManager, new FraudDataModelDerivationConfigRequestValidator()))
        .start();

    this.fraudDataModelDerivationConfigServiceBlockingStub =
        FraudDataModelDerivationConfigServiceGrpc.newBlockingStub(
                this.mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    when(uuidGenerator.generateRandomId()).thenReturn(UUID_1);
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void testDerivationConfigCRUD() {
    RequestContext requestContext = buildRequestContext();
    DerivationConfig createdDerivationConfig =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub
                    .createDerivationConfig(
                        CreateDerivationConfigRequest.newBuilder()
                            .setDerivationConfigType(
                                DerivationConfigType.DERIVATION_CONFIG_TYPE_EVENT)
                            .setName("derivation_config")
                            .setDerivationConfig("yaml_config")
                            .build())
                    .getDerivationConfig());

    assertEquals(UUID_1, createdDerivationConfig.getId());

    DerivationConfig updatedDerivationConfig =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub
                    .updateDerivationConfig(
                        UpdateDerivationConfigRequest.newBuilder()
                            .setDerivationConfig(
                                DerivationConfig.newBuilder()
                                    .setId(UUID_1)
                                    .setDerivationConfigType(
                                        DerivationConfigType.DERIVATION_CONFIG_TYPE_EVENT)
                                    .setName("derivation_config_updated")
                                    .setDerivationConfig("yaml_config_updated"))
                            .build())
                    .getDerivationConfig());

    assertEquals("derivation_config_updated", updatedDerivationConfig.getName());
    assertEquals("yaml_config_updated", updatedDerivationConfig.getDerivationConfig());

    GetDerivationConfigsResponse getDerivationConfigsResponse =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub.getDerivationConfigs(
                    GetDerivationConfigsRequest.newBuilder()
                        .setDerivationConfigType(DerivationConfigType.DERIVATION_CONFIG_TYPE_EVENT)
                        .build()));
    assertEquals(1, getDerivationConfigsResponse.getDerivationConfigsCount());

    getDerivationConfigsResponse =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub.getDerivationConfigs(
                    GetDerivationConfigsRequest.newBuilder()
                        .setDerivationConfigType(DerivationConfigType.DERIVATION_CONFIG_TYPE_ENTITY)
                        .build()));
    assertEquals(0, getDerivationConfigsResponse.getDerivationConfigsCount());

    storeManager.deleteDerivedConfig(requestContext, UUID_1);

    getDerivationConfigsResponse =
        requestContext.call(
            () ->
                this.fraudDataModelDerivationConfigServiceBlockingStub.getDerivationConfigs(
                    GetDerivationConfigsRequest.newBuilder()
                        .setDerivationConfigType(DerivationConfigType.DERIVATION_CONFIG_TYPE_EVENT)
                        .build()));
    assertEquals(0, getDerivationConfigsResponse.getDerivationConfigsCount());
  }

  private static RequestContext buildRequestContext() {
    return RequestContext.forTenantId("t1")
        .put(
            "authorization",
            "Bearer eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJGYXZpYW4uUmV5bm9sZHNAZXhhbXBsZS5jb20iLCJyb2xlIjoidXNlciIsImlhdCI6MTY4NDM0NzcxOSwiZXhwIjoxNjg0OTUyNTE5fQ.cjyK-u3j7K5sHNSf7SG0oGe6xKCV0jOTm9kqN68JmFkdxUx5Dvd1WuF7Zg7uM-8sNBgxmtPg-bxJ2Ddnmb4Fd3IhSe1L-1dbZi3-dJldlK6i9m3O5pp3ivoQvobk1mfh2jCdbZ3tLcLF7t5OLDnqK_9COQ4plTMlmwX8Jr1L8C1kfzYHfHa91Rc9lDGjSJnxaAwfXAgqBhOSZCdX-EgYRyINnF6elLibLnL8J_PP50RLctNnZuEznImvPXQ6twl8A6JJmCkJYUp0HP9mdBJIz6RcuHYt0QU5avZprKQ6p_23WhuNzSvPRSjD0l9RGR8uQHU8LWVuu9i0L8Sfyr3TrA");
  }
}
