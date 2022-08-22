package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

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
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.ModsecDetails;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import com.google.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ModsecDataFetcher {
  private static final String CRS_RULE_ID_REGEX = "^crs_";
  private static final GetScopedAnomalyGlobalConfigStatusRequest
      getScopedAnomalyGlobalConfigStatusRequest =
          GetScopedAnomalyGlobalConfigStatusRequest.newBuilder()
              .setConfigScope(
                  AnomalyConfigScope.newBuilder()
                      .setCustomerScope(AnomalyCustomerScope.getDefaultInstance()))
              .build();
  private static final GetScopedAnomalyDetectionConfigRequest
      getScopedAnomalyDetectionConfigRequest =
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
              .build();

  private final AnomalyGlobalConfigServiceBlockingStub anomalyGlobalConfigServiceStub;
  private final DetectorConfigServiceBlockingStub detectorConfigServiceBlockingStub;

  @Inject
  public ModsecDataFetcher(
      AnomalyGlobalConfigServiceBlockingStub anomalyGlobalConfigServiceStub,
      DetectorConfigServiceBlockingStub detectorConfigServiceBlockingStub) {
    this.anomalyGlobalConfigServiceStub = anomalyGlobalConfigServiceStub;
    this.detectorConfigServiceBlockingStub = detectorConfigServiceBlockingStub;
  }

  public List<BlockingDetails> getModsecViolations(RequestContext requestContext) {
    GetScopedAnomalyGlobalConfigStatusResponse statusResponse =
        anomalyGlobalConfigServiceStub.getScopedAnomalyGlobalConfigStatus(
            getScopedAnomalyGlobalConfigStatusRequest);
    List<BlockingDetails> parsedModsecViolations = List.of();
    if (!statusResponse.getScopedConfig().getConfigStatus().getDisabled()) {
      GetScopedAnomalyDetectionConfigResponse response =
          requestContext.call(
              () ->
                  detectorConfigServiceBlockingStub.getScopedAnomalyDetectionConfig(
                      getScopedAnomalyDetectionConfigRequest));

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
                          .setModsecDetails(
                              ModsecDetails.newBuilder()
                                  .setRuleId(ruleId.replaceFirst(CRS_RULE_ID_REGEX, ""))
                                  .build())
                          .build())
              .collect(Collectors.toUnmodifiableList());
    }
    return parsedModsecViolations;
  }
}
