package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
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
import ai.traceable.anomaly.config.service.v1.global.ModsecGlobalConfig;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatus;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Status;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.ModsecBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.modsecurity.utils.ModsecRuleUtils;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

class ModsecBlockingPolicyDataFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private AnomalyGlobalConfigServiceBlockingStub anomalyGlobalConfigServiceStub;
  private DetectorConfigServiceBlockingStub detectorConfigServiceBlockingStub;
  private ModsecBlockingPolicyDataFetcher modsecDataFetcher;

  @BeforeEach
  void setUp() {
    anomalyGlobalConfigServiceStub =
        mock(AnomalyGlobalConfigServiceBlockingStub.class, Answers.RETURNS_SELF);
    detectorConfigServiceBlockingStub =
        mock(DetectorConfigServiceBlockingStub.class, Answers.RETURNS_SELF);
    modsecDataFetcher =
        new ModsecBlockingPolicyDataFetcher(
            anomalyGlobalConfigServiceStub,
            detectorConfigServiceBlockingStub,
            ClientConfig.DEFAULT,
            new ModsecRuleUtils());
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
        modsecDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList();

    assertEquals(1, violations.size());
    assertEquals(
        ModsecBlockingDetails.builder().ruleId("123456").build(),
        violations.get(0).getBlockingDetails());
    assertEquals(Category.MODSECURITY, violations.get(0).getCategory());
    assertEquals(RuleType.BLOCK, violations.get(0).getRuleType());
    assertEquals(Status.DENIED, violations.get(0).getStatus());
    assertEquals(BlockingPolicyDataBucket.MODSEC_VIOLATIONS, violations.get(0).getBucket());
    assertEquals(
        ViolationInfoEncoder.getEncodedSafeCrsViolationInfo("crs_123456"),
        violations.get(0).getInfo());
    assertEquals("123456", violations.get(0).getRuleId());

    // If modsec rules are disabled
    doReturn(
            GetScopedAnomalyGlobalConfigStatusResponse.newBuilder()
                .setScopedConfig(
                    ScopedAnomalyConfigStatus.newBuilder()
                        .setModsecGlobalConfig(ModsecGlobalConfig.newBuilder().setDisabled(true)))
                .build())
        .when(anomalyGlobalConfigServiceStub)
        .getScopedAnomalyGlobalConfigStatus(
            GetScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                .setConfigScope(defaultCustomerScope)
                .build());
    List<BlockingPolicyData> violations2 =
        modsecDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList();
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
        modsecDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder()
                    .environmentId(Optional.of(ENVIRONMENT_ID))
                    .build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList();

    assertEquals(1, violations.size());
    assertEquals(
        ModsecBlockingDetails.builder().ruleId("123456").build(),
        violations.get(0).getBlockingDetails());
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
                      .setModsecGlobalConfig(ModsecGlobalConfig.newBuilder().setDisabled(false)))
              .build();
}
