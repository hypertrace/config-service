package ai.traceable.blocking.config.service.blockingpolicy.fetchers.impl;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_MODSECURITY;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_DENIED;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigResponse;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.ModsecDataFetcher;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.ModsecDetails;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import com.google.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

public class ModsecDataFetcherImpl implements ModsecDataFetcher {
  private final AnomalyGlobalConfigServiceBlockingStub anomalyGlobalConfigServiceStub;
  private final DetectorConfigServiceBlockingStub detectorConfigServiceBlockingStub;

  @Inject
  public ModsecDataFetcherImpl(
      AnomalyGlobalConfigServiceBlockingStub anomalyGlobalConfigServiceStub,
      DetectorConfigServiceBlockingStub detectorConfigServiceBlockingStub) {
    this.anomalyGlobalConfigServiceStub = anomalyGlobalConfigServiceStub;
    this.detectorConfigServiceBlockingStub = detectorConfigServiceBlockingStub;
  }

  @Override
  public List<BlockingDetails> getModsecViolations() {
    GetScopedAnomalyGlobalConfigStatusResponse statusResponse =
        anomalyGlobalConfigServiceStub.getScopedAnomalyGlobalConfigStatus(
            GetScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setCustomerScope(AnomalyCustomerScope.getDefaultInstance()))
                .build());
    List<BlockingDetails> parsedModsecViolations = List.of();
    if (!statusResponse.getScopedConfig().getConfigStatus().getDisabled()) {
      GetScopedAnomalyDetectionConfigResponse response =
          detectorConfigServiceBlockingStub.getScopedAnomalyDetectionConfig(
              GetScopedAnomalyDetectionConfigRequest.newBuilder()
                  .setConfigScope(
                      AnomalyConfigScope.newBuilder()
                          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                          .build())
                  .setFilter(
                      GetAnomalyDetectionConfigsFilter.newBuilder()
                          .addAnomalyDetectionConfigTypes(
                              AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY)
                          .build())
                  .build());

      parsedModsecViolations =
          response.getScopedAnomalyDetectionConfig().getAnomalyDetectionConfigsList().stream()
              .filter(
                  detectionConfig ->
                      detectionConfig.getModsecurityAnomalyDetectionConfig().hasModsecAnomalyRule())
              .filter(detectionConfig -> !detectionConfig.getConfigStatus().getDisabled())
              .flatMap(
                  detectionConfig ->
                      detectionConfig
                          .getModsecurityAnomalyDetectionConfig()
                          .getModsecAnomalyRule()
                          .getSubRuleConfigsList()
                          .stream())
              .filter(AnomalySubRuleConfig::getBlockingEnabled)
              .map(AnomalySubRuleConfig::getSubRuleId)
              .map(
                  ruleId ->
                      BlockingDetails.newBuilder()
                          .setCategory(BLOCKING_CATEGORY_MODSECURITY)
                          .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK)
                          .setInfo(ViolationInfoEncoder.getEncodedSafeCrsViolationInfo(ruleId))
                          .setStatus(BLOCKING_STATUS_DENIED)
                          .setModsecDetails(ModsecDetails.newBuilder().setRuleId(ruleId).build())
                          .build())
              .collect(Collectors.toUnmodifiableList());
    }
    return parsedModsecViolations;
  }
}
