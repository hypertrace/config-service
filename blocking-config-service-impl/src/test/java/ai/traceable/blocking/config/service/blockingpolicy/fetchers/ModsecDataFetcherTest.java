package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import static ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_MODSECURITY;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
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
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.impl.ModsecDataFetcherImpl;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ModsecDataFetcherTest {

  private AnomalyGlobalConfigServiceBlockingStub anomalyGlobalConfigServiceStub;
  private DetectorConfigServiceBlockingStub detectorConfigServiceBlockingStub;
  private ModsecDataFetcher modsecDataFetcher;

  @BeforeEach
  void setUp() {
    anomalyGlobalConfigServiceStub = mock(AnomalyGlobalConfigServiceBlockingStub.class);
    detectorConfigServiceBlockingStub = mock(DetectorConfigServiceBlockingStub.class);
    modsecDataFetcher =
        new ModsecDataFetcherImpl(
            anomalyGlobalConfigServiceStub, detectorConfigServiceBlockingStub);
  }

  @Test
  void getModsecViolations() {
    AnomalyConfigScope defaultCustomerScope =
        AnomalyConfigScope.newBuilder()
            .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
            .build();

    // If modsec rules are enabled
    doReturn(
            GetScopedAnomalyGlobalConfigStatusResponse.newBuilder()
                .setScopedConfig(
                    ScopedAnomalyConfigStatus.newBuilder()
                        .setConfigStatus(
                            AnomalyConfigStatus.newBuilder().setDisabled(false).build())
                        .build())
                .build())
        .when(anomalyGlobalConfigServiceStub)
        .getScopedAnomalyGlobalConfigStatus(
            GetScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                .setConfigScope(defaultCustomerScope)
                .build());

    doReturn(
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
                                                        .setSubRuleId("crs_123456")
                                                        .build())
                                                .addSubRuleConfigs(
                                                    AnomalySubRuleConfig.newBuilder()
                                                        .setBlockingEnabled(false)
                                                        .setSubRuleId("crs_111111")
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build())
        .when(detectorConfigServiceBlockingStub)
        .getScopedAnomalyDetectionConfig(
            GetScopedAnomalyDetectionConfigRequest.newBuilder()
                .setConfigScope(defaultCustomerScope)
                .setFilter(
                    GetAnomalyDetectionConfigsFilter.newBuilder()
                        .addAnomalyDetectionConfigTypes(ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY)
                        .build())
                .build());

    List<BlockingDetails> violations = modsecDataFetcher.getModsecViolations();
    assertEquals(1, violations.size());
    assertEquals("123456", violations.get(0).getModsecDetails().getRuleId());
    assertEquals(BLOCKING_CATEGORY_MODSECURITY, violations.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_BLOCK, violations.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_DENIED, violations.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedSafeCrsViolationInfo("crs_123456"),
        violations.get(0).getInfo());

    // If modsec rules are disabled
    doReturn(
            GetScopedAnomalyGlobalConfigStatusResponse.newBuilder()
                .setScopedConfig(
                    ScopedAnomalyConfigStatus.newBuilder()
                        .setConfigStatus(AnomalyConfigStatus.newBuilder().setDisabled(true).build())
                        .build())
                .build())
        .when(anomalyGlobalConfigServiceStub)
        .getScopedAnomalyGlobalConfigStatus(
            GetScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                .setConfigScope(defaultCustomerScope)
                .build());
    List<BlockingDetails> violations2 = modsecDataFetcher.getModsecViolations();
    assertEquals(0, violations2.size());
  }
}
