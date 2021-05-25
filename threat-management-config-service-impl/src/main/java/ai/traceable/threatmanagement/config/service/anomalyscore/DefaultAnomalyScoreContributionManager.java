package ai.traceable.threatmanagement.config.service.anomalyscore;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.ANOMALY_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;

import ai.traceable.threatmanagement.config.service.ThreatManagementConfigServiceConfig;
import ai.traceable.threatmanagement.config.service.v1.AnomalyScoreContribution;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DefaultAnomalyScoreContributionManager implements AnomalyScoreContributionManager {
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final ThreatManagementConfigServiceConfig config;
  private final AnomalyScoreContributionConverter anomalyScoreContributionConverter;

  @Inject
  DefaultAnomalyScoreContributionManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ThreatManagementConfigServiceConfig config,
      AnomalyScoreContributionConverter anomalyScoreContributionConverter) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.config = config;
    this.anomalyScoreContributionConverter = anomalyScoreContributionConverter;
  }

  @Override
  public AnomalyScoreContribution getAnomalyScoreContribution(RequestContext requestContext) {
    return getAnomalyScoreContributionConfig(requestContext)
        .orElseGet(this::getDefaultAnomalyScoreContribution);
  }

  @SneakyThrows
  private Optional<AnomalyScoreContribution> getAnomalyScoreContributionConfig(
      RequestContext requestContext) {
    String configId = getConfigId(requestContext);
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(ANOMALY_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME)
            .addContexts(configId)
            .build();

    try {
      Value config =
          requestContext.call(() -> configServiceBlockingStub.getConfig(request).getConfig());

      return anomalyScoreContributionConverter.convert(config);
    } catch (Exception e) {
      if (Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw e;
    }
  }

  @Override
  public AnomalyScoreContribution upsertAnomalyScoreContribution(
      RequestContext requestContext, AnomalyScoreContribution anomalyScoreContribution) {
    return upsertAnomalyScoreContributionConfig(requestContext, anomalyScoreContribution)
        .orElseThrow(Status.INTERNAL::asRuntimeException);
  }

  @SneakyThrows
  private Optional<AnomalyScoreContribution> upsertAnomalyScoreContributionConfig(
      RequestContext requestContext, AnomalyScoreContribution anomalyScoreContribution) {
    String configId = getConfigId(requestContext);

    UpsertConfigRequest request =
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(ANOMALY_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME)
            .setConfig(anomalyScoreContributionConverter.convert(anomalyScoreContribution))
            .setContext(configId)
            .build();

    UpsertConfigResponse response = configServiceBlockingStub.upsertConfig(request);
    return anomalyScoreContributionConverter.convert(response.getConfig());
  }

  @Override
  public AnomalyScoreContribution getDefaultAnomalyScoreContribution() {
    return AnomalyScoreContribution.newBuilder()
        .setAnomalyScore(this.config.getDefaultAnomalyContributionScore())
        .build();
  }

  private String getConfigId(RequestContext requestContext) {
    // Using tenant id as anomaly score contribution config id, since it's tenant scoped
    return requestContext
        .getTenantId()
        .orElseThrow(
            () -> new IllegalArgumentException("Unable to get config id from request context"));
  }
}
