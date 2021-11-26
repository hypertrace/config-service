package ai.traceable.config.service.anomaly;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.trainer.GetAllScopedTrainingConfigsRequest;
import ai.traceable.anomaly.config.service.v1.trainer.GetScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.LackOfEncryptionTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.MinOccurrenceConfig;
import ai.traceable.anomaly.config.service.v1.trainer.ScopedTrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.TrainerConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.trainer.TrainingConfig;
import ai.traceable.anomaly.config.service.v1.trainer.UpdateScopedTrainingConfigRequest;
import ai.traceable.anomaly.config.service.v1.trainer.VulnerabilityTrainingConfig;
import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TrainerConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {
  private static TrainerConfigServiceGrpc.TrainerConfigServiceBlockingStub configServiceStub;
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
        TrainerConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testGetAndUpdateScopedTrainingConfig() {
    ScopedTrainingConfig scopedTrainingConfig;
    assertThrows(
        RuntimeException.class,
        () ->
            fetchTrainerConfig(
                AnomalyConfigScope.newBuilder()
                    .setParamScope(AnomalyParamScope.getDefaultInstance())
                    .build()));

    assertThrows(
        RuntimeException.class,
        () ->
            updateTrainerConfig(
                ScopedTrainingConfig.newBuilder()
                    .setConfigScope(
                        AnomalyConfigScope.newBuilder()
                            .setParamScope(AnomalyParamScope.getDefaultInstance())
                            .build())
                    .build()));

    scopedTrainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(customerConfigScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setVulnerabilityTrainingConfig(
                        VulnerabilityTrainingConfig.newBuilder()
                            .setLackOfEncryption(
                                LackOfEncryptionTrainingConfig.newBuilder()
                                    .setHttpsCallsConfig(
                                        MinOccurrenceConfig.newBuilder()
                                            .setMinTotalOccurrences(50)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    updateTrainerConfig(scopedTrainingConfig);
    assertEquals(
        50,
        fetchTrainerConfig(customerConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        50,
        fetchTrainerConfig(serviceConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        50,
        fetchTrainerConfig(apiConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());

    scopedTrainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(serviceConfigScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setVulnerabilityTrainingConfig(
                        VulnerabilityTrainingConfig.newBuilder()
                            .setLackOfEncryption(
                                LackOfEncryptionTrainingConfig.newBuilder()
                                    .setHttpsCallsConfig(
                                        MinOccurrenceConfig.newBuilder()
                                            .setMinTotalOccurrences(60)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    updateTrainerConfig(scopedTrainingConfig);
    // customer config remains unchanged
    assertEquals(
        50,
        fetchTrainerConfig(customerConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        60,
        fetchTrainerConfig(serviceConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        60,
        fetchTrainerConfig(apiConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());

    scopedTrainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(apiConfigScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setVulnerabilityTrainingConfig(
                        VulnerabilityTrainingConfig.newBuilder()
                            .setLackOfEncryption(
                                LackOfEncryptionTrainingConfig.newBuilder()
                                    .setHttpsCallsConfig(
                                        MinOccurrenceConfig.newBuilder()
                                            .setMinTotalOccurrences(70)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    updateTrainerConfig(scopedTrainingConfig);
    // customer config remains unchanged
    assertEquals(
        50,
        fetchTrainerConfig(customerConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    // service config remains unchanged
    assertEquals(
        60,
        fetchTrainerConfig(serviceConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        70,
        fetchTrainerConfig(apiConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
  }

  @Test
  void testGetAllScopedTrainingConfigs() {
    ScopedTrainingConfig scopedTrainingConfig;

    scopedTrainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(customerConfigScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setVulnerabilityTrainingConfig(
                        VulnerabilityTrainingConfig.newBuilder()
                            .setLackOfEncryption(
                                LackOfEncryptionTrainingConfig.newBuilder()
                                    .setHttpsCallsConfig(
                                        MinOccurrenceConfig.newBuilder()
                                            .setMinTotalOccurrences(50)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    updateTrainerConfig(scopedTrainingConfig);

    List<ScopedTrainingConfig> scopedTrainingConfigs = fetchAllTrainerConfigs(TENANT_ID);

    assertEquals(1, scopedTrainingConfigs.size());
    assertEquals(
        50,
        getScopedTrainingConfig(scopedTrainingConfigs, customerConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());

    scopedTrainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(serviceConfigScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setVulnerabilityTrainingConfig(
                        VulnerabilityTrainingConfig.newBuilder()
                            .setLackOfEncryption(
                                LackOfEncryptionTrainingConfig.newBuilder()
                                    .setHttpsCallsConfig(
                                        MinOccurrenceConfig.newBuilder()
                                            .setMinTotalOccurrences(60)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    updateTrainerConfig(scopedTrainingConfig);

    scopedTrainingConfigs = fetchAllTrainerConfigs(TENANT_ID);
    assertEquals(2, scopedTrainingConfigs.size());
    assertEquals(
        50,
        getScopedTrainingConfig(scopedTrainingConfigs, customerConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        60,
        getScopedTrainingConfig(scopedTrainingConfigs, serviceConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());

    scopedTrainingConfig =
        ScopedTrainingConfig.newBuilder()
            .setConfigScope(apiConfigScope)
            .addTrainingConfigs(
                TrainingConfig.newBuilder()
                    .setVulnerabilityTrainingConfig(
                        VulnerabilityTrainingConfig.newBuilder()
                            .setLackOfEncryption(
                                LackOfEncryptionTrainingConfig.newBuilder()
                                    .setHttpsCallsConfig(
                                        MinOccurrenceConfig.newBuilder()
                                            .setMinTotalOccurrences(70)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    updateTrainerConfig(scopedTrainingConfig);

    scopedTrainingConfigs = fetchAllTrainerConfigs(TENANT_ID);
    assertEquals(3, scopedTrainingConfigs.size());
    assertEquals(
        50,
        getScopedTrainingConfig(scopedTrainingConfigs, customerConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        60,
        getScopedTrainingConfig(scopedTrainingConfigs, serviceConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
    assertEquals(
        70,
        getScopedTrainingConfig(scopedTrainingConfigs, apiConfigScope)
            .getTrainingConfigsList()
            .get(0)
            .getVulnerabilityTrainingConfig()
            .getLackOfEncryption()
            .getHttpsCallsConfig()
            .getMinTotalOccurrences());
  }

  private ScopedTrainingConfig fetchTrainerConfig(AnomalyConfigScope configScope) {
    return fetchTrainerConfig(configScope, TENANT_ID);
  }

  private ScopedTrainingConfig fetchTrainerConfig(AnomalyConfigScope configScope, String tenantId) {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        tenantId,
        () ->
            configServiceStub
                .getScopedTrainingConfig(
                    GetScopedTrainingConfigRequest.newBuilder().setConfigScope(configScope).build())
                .getScopedTrainingConfig());
  }

  private List<ScopedTrainingConfig> fetchAllTrainerConfigs(String tenantId) {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        tenantId,
        () ->
            configServiceStub
                .getAllScopedTrainingConfigs(
                    GetAllScopedTrainingConfigsRequest.newBuilder().build())
                .getScopedTrainingConfigsList());
  }

  private ScopedTrainingConfig updateTrainerConfig(ScopedTrainingConfig scopedTrainingConfig) {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            configServiceStub
                .updateScopedTrainingConfig(
                    UpdateScopedTrainingConfigRequest.newBuilder()
                        .setScopedTrainingConfig(scopedTrainingConfig)
                        .build())
                .getScopedTrainingConfig());
  }

  private ScopedTrainingConfig getScopedTrainingConfig(
      List<ScopedTrainingConfig> trainingConfigs, AnomalyConfigScope configScope) {
    for (ScopedTrainingConfig trainingConfig : trainingConfigs) {
      if (trainingConfig.getConfigScope().equals(configScope)) {
        return trainingConfig;
      }
    }
    return null;
  }
}
