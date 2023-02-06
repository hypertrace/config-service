package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigResponse;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ModsecurityAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Status;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ModsecDataFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private AnomalyGlobalConfigServiceBlockingStub anomalyGlobalConfigServiceStub;
  private DetectorConfigServiceBlockingStub detectorConfigServiceBlockingStub;
  private ModsecDataFetcher modsecDataFetcher;

  @BeforeEach
  void setUp() {
    anomalyGlobalConfigServiceStub = mock(AnomalyGlobalConfigServiceBlockingStub.class);
    detectorConfigServiceBlockingStub = mock(DetectorConfigServiceBlockingStub.class);
    modsecDataFetcher =
        new ModsecDataFetcher(anomalyGlobalConfigServiceStub, detectorConfigServiceBlockingStub);
  }

  @Test
  void getModsecViolationsWithoutEnvironment() {
    AnomalyConfigScope defaultCustomerScope =
        AnomalyConfigScope.newBuilder()
            .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
            .build();

    // If modsec rules are enabled
    doReturn(sampleAnomalyGlobalConfigStatusResponse)
        .when(anomalyGlobalConfigServiceStub)
        .getScopedAnomalyGlobalConfigStatus(
            GetScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                .setConfigScope(defaultCustomerScope)
                .build());

    doReturn(sampleAnomalyDetectionConfigResponse)
        .when(detectorConfigServiceBlockingStub)
        .getScopedAnomalyDetectionConfig(
            GetScopedAnomalyDetectionConfigRequest.newBuilder()
                .setConfigScope(defaultCustomerScope)
                .setFilter(
                    GetAnomalyDetectionConfigsFilter.newBuilder()
                        .addAnomalyDetectionConfigTypes(ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY))
                .build());

    List<BlockingPolicyData> violations =
        modsecDataFetcher.getModsecViolations(REQUEST_CONTEXT, Optional.empty());

    assertEquals(1, violations.size());
    assertEquals("123456", violations.get(0).getRuleId());
    assertEquals(Category.MODSECURITY, violations.get(0).getCategory());
    assertEquals(RuleType.BLOCK, violations.get(0).getRuleType());
    assertEquals(Status.DENIED, violations.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedSafeCrsViolationInfo("crs_123456"),
        violations.get(0).getInfo());

    // If modsec rules are disabled
    doReturn(
            GetScopedAnomalyGlobalConfigStatusResponse.newBuilder()
                .setScopedConfig(
                    ScopedAnomalyConfigStatus.newBuilder()
                        .setConfigStatus(
                            AnomalyConfigStatus.newBuilder().setDisabled(true).build()))
                .build())
        .when(anomalyGlobalConfigServiceStub)
        .getScopedAnomalyGlobalConfigStatus(
            GetScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                .setConfigScope(defaultCustomerScope)
                .build());
    List<BlockingPolicyData> violations2 =
        modsecDataFetcher.getModsecViolations(REQUEST_CONTEXT, Optional.empty());
    assertEquals(0, violations2.size());
  }

  @Test
  void getModsecViolationsWithEnvironment() {
    AnomalyConfigScope environmentScopedAnomalyConfig =
        AnomalyConfigScope.newBuilder()
            .setEnvironmentScope(
                AnomalyEnvironmentScope.newBuilder().setEnvironmentId(ENVIRONMENT_ID))
            .build();

    // If modsec rules are enabled
    doReturn(sampleAnomalyGlobalConfigStatusResponse)
        .when(anomalyGlobalConfigServiceStub)
        .getScopedAnomalyGlobalConfigStatus(
            GetScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                .setConfigScope(environmentScopedAnomalyConfig)
                .build());

    doReturn(sampleAnomalyDetectionConfigResponse)
        .when(detectorConfigServiceBlockingStub)
        .getScopedAnomalyDetectionConfig(
            GetScopedAnomalyDetectionConfigRequest.newBuilder()
                .setConfigScope(environmentScopedAnomalyConfig)
                .setFilter(
                    GetAnomalyDetectionConfigsFilter.newBuilder()
                        .addAnomalyDetectionConfigTypes(ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY))
                .build());

    List<BlockingPolicyData> violations =
        modsecDataFetcher.getModsecViolations(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    assertEquals(1, violations.size());
    assertEquals("123456", violations.get(0).getRuleId());
  }

  private static final GetScopedAnomalyDetectionConfigResponse
      sampleAnomalyDetectionConfigResponse =
          GetScopedAnomalyDetectionConfigResponse.newBuilder()
              .setScopedAnomalyDetectionConfig(
                  ScopedAnomalyDetectionConfig.newBuilder()
                      .addAnomalyDetectionConfigs(
                          AnomalyDetectionConfig.newBuilder()
                              .setModsecurityAnomalyDetectionConfig(
                                  ModsecurityAnomalyDetectionConfig.newBuilder()
                                      .setModsecAnomalyRule(
                                          ModsecurityAnomalyRuleConfig.newBuilder()
                                              .addSubRuleConfigs(
                                                  AnomalySubRuleConfig.newBuilder()
                                                      .setBlockingEnabled(true)
                                                      .setSubRuleId("crs_123456"))
                                              .addSubRuleConfigs(
                                                  AnomalySubRuleConfig.newBuilder()
                                                      .setBlockingEnabled(false)
                                                      .setSubRuleId("crs_111111"))))))
              .build();

  private static final GetScopedAnomalyGlobalConfigStatusResponse
      sampleAnomalyGlobalConfigStatusResponse =
          GetScopedAnomalyGlobalConfigStatusResponse.newBuilder()
              .setScopedConfig(
                  ScopedAnomalyConfigStatus.newBuilder()
                      .setConfigStatus(AnomalyConfigStatus.newBuilder().setDisabled(false).build()))
              .build();
}
