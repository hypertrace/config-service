package ai.traceable.config.service.anomaly;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistryImpl;
import ai.traceable.anomaly.config.service.registry.common.ConfigConverter;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionApplierConfig;
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.apidef.ApiDefinitionTrainerConfig;
import ai.traceable.anomaly.config.service.v1.apidef.GetApiDefinitionTrainerConfigsRequest;
import ai.traceable.anomaly.config.service.v1.apidef.QueryParamContainsSensitiveDataVulnerabilityApplierConfig;
import ai.traceable.anomaly.config.service.v1.apidef.UpdateApiDefinitionTrainerConfigsRequest;
import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ApiDefinitionConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static ApiDefinitionConfigServiceGrpc.ApiDefinitionConfigServiceBlockingStub
      configServiceStub;
  private ApiDefinitionRegistry registry = new ApiDefinitionRegistryImpl(new ConfigConverter());
  private final ApiDefinitionTrainerConfig defaultConfig = registry.getApiDefinitionTrainerConfig();
  private final AnomalyServiceScope serviceScope =
      AnomalyServiceScope.newBuilder().setId("service").build();
  private final AnomalyApiScope apiScope =
      AnomalyApiScope.newBuilder().setId("api").setServiceScope(serviceScope).build();

  private final AnomalyConfigScope customerConfigScope =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();
  private final AnomalyConfigScope serviceConfigScope =
      AnomalyConfigScope.newBuilder().setServiceScope(serviceScope).build();
  private final AnomalyConfigScope apiConfigScope =
      AnomalyConfigScope.newBuilder().setApiScope(apiScope).build();

  @BeforeAll
  static void init() {
    configServiceStub =
        ApiDefinitionConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testGetApiDefinitionTrainerConfig() {
    ApiDefinitionApplierConfig applierConfig;
    assertThrows(
        RuntimeException.class,
        () ->
            fetchApiDefinitionTrainerConfig(
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build()));
    assertEquals(defaultConfig, fetchApiDefinitionTrainerConfig(customerConfigScope, "tenant"));
    assertEquals(defaultConfig, fetchApiDefinitionTrainerConfig(customerConfigScope));

    applierConfig =
        ApiDefinitionApplierConfig.newBuilder()
            .setQueryParamContainsSensitiveData(
                QueryParamContainsSensitiveDataVulnerabilityApplierConfig.newBuilder()
                    .setMinPercentOfPiiType(50)
                    .build())
            .build();
    updateApiDefinitionTrainerConfig(customerConfigScope, List.of(applierConfig));
    assertEquals(50, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(customerConfigScope)));
    assertEquals(50, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(serviceConfigScope)));
    assertEquals(50, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(apiConfigScope)));

    applierConfig =
        ApiDefinitionApplierConfig.newBuilder()
            .setQueryParamContainsSensitiveData(
                QueryParamContainsSensitiveDataVulnerabilityApplierConfig.newBuilder()
                    .setMinPercentOfPiiType(60)
                    .build())
            .build();
    updateApiDefinitionTrainerConfig(serviceConfigScope, List.of(applierConfig));
    // customer config remains unchanged
    assertEquals(50, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(customerConfigScope)));
    assertEquals(60, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(serviceConfigScope)));
    assertEquals(60, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(apiConfigScope)));

    applierConfig =
        ApiDefinitionApplierConfig.newBuilder()
            .setQueryParamContainsSensitiveData(
                QueryParamContainsSensitiveDataVulnerabilityApplierConfig.newBuilder()
                    .setMinPercentOfPiiType(70)
                    .build())
            .build();
    updateApiDefinitionTrainerConfig(apiConfigScope, List.of(applierConfig));
    // customer config remains unchanged
    assertEquals(50, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(customerConfigScope)));
    // service config remains unchanged
    assertEquals(60, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(serviceConfigScope)));
    assertEquals(70, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(apiConfigScope)));
  }

  @Test
  void testUpdateApiDefinitionTrainerConfig() {
    ApiDefinitionApplierConfig applierConfig;
    assertThrows(
        RuntimeException.class,
        () ->
            updateApiDefinitionTrainerConfig(
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build(),
                List.of(ApiDefinitionApplierConfig.getDefaultInstance())));

    applierConfig =
        ApiDefinitionApplierConfig.newBuilder()
            .setQueryParamContainsSensitiveData(
                QueryParamContainsSensitiveDataVulnerabilityApplierConfig.newBuilder()
                    .setMinPercentOfPiiType(50)
                    .build())
            .build();
    assertEquals(
        50,
        getQueryParamMinPercent(
            updateApiDefinitionTrainerConfig(customerConfigScope, List.of(applierConfig))));
    assertEquals(50, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(customerConfigScope)));
    assertEquals(50, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(serviceConfigScope)));
    assertEquals(50, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(apiConfigScope)));

    applierConfig =
        ApiDefinitionApplierConfig.newBuilder()
            .setQueryParamContainsSensitiveData(
                QueryParamContainsSensitiveDataVulnerabilityApplierConfig.newBuilder()
                    .setMinPercentOfPiiType(60)
                    .build())
            .build();
    assertEquals(
        60,
        getQueryParamMinPercent(
            updateApiDefinitionTrainerConfig(serviceConfigScope, List.of(applierConfig))));
    // customer config remains unchanged
    assertEquals(50, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(customerConfigScope)));
    assertEquals(60, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(serviceConfigScope)));
    assertEquals(60, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(apiConfigScope)));

    applierConfig =
        ApiDefinitionApplierConfig.newBuilder()
            .setQueryParamContainsSensitiveData(
                QueryParamContainsSensitiveDataVulnerabilityApplierConfig.newBuilder()
                    .setMinPercentOfPiiType(70)
                    .build())
            .build();
    assertEquals(
        70,
        getQueryParamMinPercent(
            updateApiDefinitionTrainerConfig(apiConfigScope, List.of(applierConfig))));
    // customer config remains unchanged
    assertEquals(50, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(customerConfigScope)));
    // service config remains unchanged
    assertEquals(60, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(serviceConfigScope)));
    assertEquals(70, getQueryParamMinPercent(fetchApiDefinitionTrainerConfig(apiConfigScope)));
  }

  private ApiDefinitionTrainerConfig fetchApiDefinitionTrainerConfig(
      AnomalyConfigScope configScope) {
    return fetchApiDefinitionTrainerConfig(configScope, TENANT_ID);
  }

  private ApiDefinitionTrainerConfig fetchApiDefinitionTrainerConfig(
      AnomalyConfigScope configScope, String tenantId) {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        tenantId,
        () ->
            configServiceStub
                .getApiDefinitionTrainerConfigs(
                    GetApiDefinitionTrainerConfigsRequest.newBuilder()
                        .setConfigScope(configScope)
                        .build())
                .getApiDefinitionTrainerConfig());
  }

  private ApiDefinitionTrainerConfig updateApiDefinitionTrainerConfig(
      AnomalyConfigScope configScope, List<ApiDefinitionApplierConfig> applierConfigs) {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            configServiceStub
                .updateApiDefinitionTrainerConfigs(
                    UpdateApiDefinitionTrainerConfigsRequest.newBuilder()
                        .setConfigScope(configScope)
                        .addAllApiDefinitionApplierConfigs(applierConfigs)
                        .build())
                .getApiDefinitionTrainerConfig());
  }

  private int getQueryParamMinPercent(ApiDefinitionTrainerConfig config) {
    for (ApiDefinitionApplierConfig applierConfig : config.getApiDefinitionApplierConfigsList()) {
      if (applierConfig.getApplierConfigCase()
          == ApiDefinitionApplierConfig.ApplierConfigCase.QUERY_PARAM_CONTAINS_SENSITIVE_DATA) {
        return applierConfig.getQueryParamContainsSensitiveData().getMinPercentOfPiiType();
      }
    }
    throw new RuntimeException("QueryParamContainsSensitiveData applier does not exist in config");
  }
}
