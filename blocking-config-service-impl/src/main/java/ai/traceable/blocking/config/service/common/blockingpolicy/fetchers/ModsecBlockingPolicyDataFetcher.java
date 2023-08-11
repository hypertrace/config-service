package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfigType;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.DetectorConfigServiceGrpc.DetectorConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.detector.GetAnomalyDetectionConfigsFilter;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigRequest;
import ai.traceable.anomaly.config.service.v1.detector.GetScopedAnomalyDetectionConfigResponse;
import ai.traceable.anomaly.config.service.v1.global.AnomalyGlobalConfigServiceGrpc.AnomalyGlobalConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusResponse;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Status;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.ModsecBlockingDetails;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

class ModsecBlockingPolicyDataFetcher implements BlockingPolicyDataFetcherBase {
  private static final String CRS_RULE_ID_REGEX = "^crs_";
  private static final AnomalyConfigScope DEFAULT_ANOMALY_CONFIG_SCOPE =
      AnomalyConfigScope.newBuilder()
          .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
          .build();

  private final AnomalyGlobalConfigServiceBlockingStub anomalyGlobalConfigServiceStub;
  private final DetectorConfigServiceBlockingStub detectorConfigServiceBlockingStub;

  @Inject
  ModsecBlockingPolicyDataFetcher(
      AnomalyGlobalConfigServiceBlockingStub anomalyGlobalConfigServiceStub,
      DetectorConfigServiceBlockingStub detectorConfigServiceBlockingStub) {
    this.anomalyGlobalConfigServiceStub = anomalyGlobalConfigServiceStub;
    this.detectorConfigServiceBlockingStub = detectorConfigServiceBlockingStub;
  }

  @Override
  public BlockingPolicyAggregate<BlockingPolicyData> getBlockingPolicyData(
      RequestContext requestContext,
      BlockingPolicyDataFilter filter,
      BlockingRulesSupplier blockingRulesSupplier) {
    Optional<String> environmentId = filter.getEnvironmentId();
    AnomalyConfigScope anomalyConfigScope =
        environmentId
            .map(
                id ->
                    AnomalyConfigScope.newBuilder()
                        .setEnvironmentScope(
                            AnomalyEnvironmentScope.newBuilder().setEnvironmentId(id))
                        .build())
            .orElse(DEFAULT_ANOMALY_CONFIG_SCOPE);

    GetScopedAnomalyGlobalConfigStatusResponse statusResponse =
        getScopedAnomalyGlobalConfigStatus(requestContext, anomalyConfigScope);
    if (!statusResponse.getScopedConfig().getConfigStatus().getDisabled()) {
      return new BlockingPolicyAggregate(
          parseModsecViolations(
              getScopedAnomalyDetectionConfig(requestContext, anomalyConfigScope)));
    }
    return new BlockingPolicyAggregate(Collections.emptyList());
  }

  private GetScopedAnomalyGlobalConfigStatusResponse getScopedAnomalyGlobalConfigStatus(
      RequestContext requestContext, AnomalyConfigScope anomalyConfigScope) {
    GetScopedAnomalyGlobalConfigStatusRequest getScopedAnomalyGlobalConfigStatusRequest =
        GetScopedAnomalyGlobalConfigStatusRequest.newBuilder()
            .setConfigScope(anomalyConfigScope)
            .build();

    return requestContext.call(
        () ->
            anomalyGlobalConfigServiceStub.getScopedAnomalyGlobalConfigStatus(
                getScopedAnomalyGlobalConfigStatusRequest));
  }

  private GetScopedAnomalyDetectionConfigResponse getScopedAnomalyDetectionConfig(
      RequestContext requestContext, AnomalyConfigScope anomalyConfigScope) {
    GetScopedAnomalyDetectionConfigRequest getScopedAnomalyDetectionConfigRequest =
        GetScopedAnomalyDetectionConfigRequest.newBuilder()
            .setConfigScope(anomalyConfigScope)
            .setFilter(
                GetAnomalyDetectionConfigsFilter.newBuilder()
                    .addAnomalyDetectionConfigTypes(
                        AnomalyDetectionConfigType.ANOMALY_DETECTION_CONFIG_TYPE_MODSECURITY))
            .build();

    return requestContext.call(
        () ->
            detectorConfigServiceBlockingStub.getScopedAnomalyDetectionConfig(
                getScopedAnomalyDetectionConfigRequest));
  }

  private static List<BlockingPolicyData> parseModsecViolations(
      GetScopedAnomalyDetectionConfigResponse scopedAnomalyDetectionConfigResponse) {
    return scopedAnomalyDetectionConfigResponse
        .getScopedAnomalyDetectionConfig()
        .getAnomalyDetectionConfigsList()
        .stream()
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
        .map(ModsecBlockingPolicyDataFetcher::generateBlockingDetails)
        .collect(Collectors.toUnmodifiableList());
  }

  private static BlockingPolicyData generateBlockingDetails(String ruleId) {
    return BlockingPolicyData.builder()
        .category(Category.MODSECURITY)
        .bucket(BlockingPolicyDataBucket.MODSEC_VIOLATIONS)
        .ruleType(RuleType.BLOCK)
        .info(ViolationInfoEncoder.getEncodedSafeCrsViolationInfo(ruleId))
        .status(Status.DENIED)
        .blockingDetails(
            ModsecBlockingDetails.builder()
                .ruleId(ruleId.replaceFirst(CRS_RULE_ID_REGEX, ""))
                .build())
        .build();
  }
}
