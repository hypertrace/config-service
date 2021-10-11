package ai.traceable.anomaly.config.service.apidef.trainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistryImpl;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScopeType;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionApplierConfig;
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionTrainerConfig;
import ai.traceable.anomaly.config.service.v1.apidef.LackOfEncryptionVulnerabilityApplierConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ApiDefinitionTrainerConfigManagerTest {

  private static Server mockServer;
  private static MockGenericConfigService mockConfigService;
  private static Channel channelForMockServer;
  private ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub;
  private final ApiDefinitionRegistry apiDefinitionRegistry =
      new ApiDefinitionRegistryImpl(new ConfigConverter());

  private ApiDefinitionTrainerConfigConverter configConverter;
  private ApiDefinitionTrainerConfigManager configManager;

  private final AnomalyServiceScope serviceScope =
      AnomalyServiceScope.newBuilder().setId("service").build();
  private final AnomalyApiScope apiScope =
      AnomalyApiScope.newBuilder().setId("api").setServiceScope(serviceScope).build();

  private final AnomalyConfigScope customerConfigScope =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();
  private final AnomalyConfigScope serviceConfigScope =
      AnomalyConfigScope.newBuilder()
          .setScopeType(AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_SERVICE)
          .setServiceScope(serviceScope)
          .build();
  private final AnomalyConfigScope apiConfigScope =
      AnomalyConfigScope.newBuilder()
          .setScopeType(AnomalyConfigScopeType.ANOMALY_CONFIG_SCOPE_TYPE_API)
          .setApiScope(apiScope)
          .build();
  private final ApiDefinitionTrainerConfig defaultConfig =
      apiDefinitionRegistry.getApiDefinitionTrainerConfig();

  @BeforeEach
  public void setup() throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    channelForMockServer = InProcessChannelBuilder.forName(serverName).build();
    mockServer = InProcessServerBuilder.forName(serverName).build().start();
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    configConverter = new ApiDefinitionTrainerConfigConverter();
    this.configManager =
        spy(
            new ApiDefinitionTrainerConfigManager(
                apiDefinitionRegistry,
                ApiDefinitionTrainerConfig.newBuilder().build(),
                configConverter,
                configServiceBlockingStub));
  }

  @AfterEach
  public void teardown() {
    mockConfigService.shutdown();
    mockServer.shutdown();
  }

  @Test
  void testGetApiDefinitionTrainerConfig() throws InvalidProtocolBufferException {
    String tenantId = "tenant";
    RequestContext requestContext = RequestContext.forTenantId(tenantId);
    ApiDefinitionTrainerConfig trainerConfig;

    assertThrows(
        RuntimeException.class,
        () ->
            configManager.getApiDefinitionTrainerConfig(
                requestContext,
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build()));

    assertEquals(
        defaultConfig,
        configManager.getApiDefinitionTrainerConfig(requestContext, customerConfigScope));

    trainerConfig =
        ApiDefinitionTrainerConfig.newBuilder()
            .addApiDefinitionApplierConfigs(
                ApiDefinitionApplierConfig.newBuilder()
                    .setLackOfEncryption(
                        LackOfEncryptionVulnerabilityApplierConfig.newBuilder()
                            .setMinNumberOfCalls(500)
                            .build())
                    .build())
            .build();
    upsertCustomerConfig(trainerConfig, tenantId);
    assertEquals(
        500,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, customerConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
    assertEquals(
        500,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, serviceConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
    assertEquals(
        500,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, apiConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());

    trainerConfig =
        ApiDefinitionTrainerConfig.newBuilder()
            .addApiDefinitionApplierConfigs(
                ApiDefinitionApplierConfig.newBuilder()
                    .setLackOfEncryption(
                        LackOfEncryptionVulnerabilityApplierConfig.newBuilder()
                            .setMinNumberOfCalls(600)
                            .build())
                    .build())
            .build();
    upsertApiConfig(trainerConfig);
    assertEquals(
        600,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, apiConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
    // customer config stays unchanged
    assertEquals(
        500,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, customerConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
    // service config stays unchanged
    assertEquals(
        500,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, serviceConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());

    trainerConfig =
        ApiDefinitionTrainerConfig.newBuilder()
            .addApiDefinitionApplierConfigs(
                ApiDefinitionApplierConfig.newBuilder()
                    .setLackOfEncryption(
                        LackOfEncryptionVulnerabilityApplierConfig.newBuilder()
                            .setMinNumberOfCalls(700)
                            .build())
                    .build())
            .build();
    upsertServiceConfig(trainerConfig);
    assertEquals(
        600,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, apiConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
    // customer config stays unchanged
    assertEquals(
        500,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, customerConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
    assertEquals(
        700,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, serviceConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
  }

  @Test
  void testUpdateApiDefinitionTrainerConfig() {
    RequestContext requestContext = RequestContext.forTenantId("update_tenant");
    assertThrows(
        RuntimeException.class,
        () ->
            configManager.updateApiDefinitionTrainerConfig(
                requestContext,
                List.of(ApiDefinitionApplierConfig.getDefaultInstance()),
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build()));
    ApiDefinitionApplierConfig applierConfig;

    applierConfig =
        ApiDefinitionApplierConfig.newBuilder()
            .setLackOfEncryption(
                LackOfEncryptionVulnerabilityApplierConfig.newBuilder()
                    .setMinNumberOfCalls(500)
                    .build())
            .build();
    assertEquals(
        500,
        getApplierConfig(
                configManager.updateApiDefinitionTrainerConfig(
                    requestContext, List.of(applierConfig), customerConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
    assertEquals(
        500,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, customerConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
    assertEquals(
        500,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, serviceConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
    assertEquals(
        500,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, apiConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());

    applierConfig =
        ApiDefinitionApplierConfig.newBuilder()
            .setLackOfEncryption(
                LackOfEncryptionVulnerabilityApplierConfig.newBuilder()
                    .setMinNumberOfCalls(600)
                    .build())
            .build();
    assertEquals(
        600,
        getApplierConfig(
                configManager.updateApiDefinitionTrainerConfig(
                    requestContext, List.of(applierConfig), serviceConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
    assertEquals(
        500,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, customerConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
    assertEquals(
        600,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, serviceConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
    assertEquals(
        600,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, apiConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());

    applierConfig =
        ApiDefinitionApplierConfig.newBuilder()
            .setLackOfEncryption(
                LackOfEncryptionVulnerabilityApplierConfig.newBuilder()
                    .setMinNumberOfCalls(700)
                    .build())
            .build();
    assertEquals(
        700,
        getApplierConfig(
                configManager.updateApiDefinitionTrainerConfig(
                    requestContext, List.of(applierConfig), apiConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
    assertEquals(
        500,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, customerConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
    assertEquals(
        600,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, serviceConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
    assertEquals(
        700,
        getApplierConfig(
                configManager.getApiDefinitionTrainerConfig(requestContext, apiConfigScope),
                ApiDefinitionApplierConfig.ApplierConfigCase.LACK_OF_ENCRYPTION)
            .getLackOfEncryption()
            .getMinNumberOfCalls());
  }

  private void upsertCustomerConfig(ApiDefinitionTrainerConfig config, String tenantId)
      throws InvalidProtocolBufferException {
    configServiceBlockingStub.upsertConfig(
        UpsertConfigRequest.newBuilder()
            .setContext(tenantId)
            .setResourceNamespace(
                ApiDefinitionTrainerConfigServiceConstants.APIDEF_TRAINER_CONFIG_NAMESPACE)
            .setResourceName(
                ApiDefinitionTrainerConfigServiceConstants.APIDEF_TRAINER_CONFIG_RESOURCE_NAME)
            .setConfig(configConverter.convert(config))
            .build());
  }

  private void upsertApiConfig(ApiDefinitionTrainerConfig config)
      throws InvalidProtocolBufferException {
    configServiceBlockingStub.upsertConfig(
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(
                ApiDefinitionTrainerConfigServiceConstants.APIDEF_TRAINER_CONFIG_NAMESPACE)
            .setResourceName(
                ApiDefinitionTrainerConfigServiceConstants.APIDEF_TRAINER_CONFIG_RESOURCE_NAME)
            .setConfig(configConverter.convert(config))
            .setContext(apiScope.getId())
            .build());
  }

  private void upsertServiceConfig(ApiDefinitionTrainerConfig config)
      throws InvalidProtocolBufferException {
    configServiceBlockingStub.upsertConfig(
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(
                ApiDefinitionTrainerConfigServiceConstants.APIDEF_TRAINER_CONFIG_NAMESPACE)
            .setResourceName(
                ApiDefinitionTrainerConfigServiceConstants.APIDEF_TRAINER_CONFIG_RESOURCE_NAME)
            .setConfig(configConverter.convert(config))
            .setContext(serviceScope.getId())
            .build());
  }

  private ApiDefinitionApplierConfig getApplierConfig(
      ApiDefinitionTrainerConfig config,
      ApiDefinitionApplierConfig.ApplierConfigCase applierConfigCase) {
    for (ApiDefinitionApplierConfig applierConfig : config.getApiDefinitionApplierConfigsList()) {
      if (applierConfig.getApplierConfigCase() == applierConfigCase) {
        return applierConfig;
      }
    }
    throw new RuntimeException(applierConfigCase + " applier does not exist in config");
  }
}
