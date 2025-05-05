package ai.traceable.threatmanagement.config.service.anomalyscore;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.ANOMALY_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;

import ai.traceable.threatmanagement.config.service.ThreatManagementConfigServiceConfig;
import ai.traceable.threatmanagement.config.service.v1.AnomalyScoreContribution;
import ai.traceable.threatmanagement.config.service.v1.ScopeConfig;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DefaultAnomalyScoreContributionManager extends IdentifiedObjectStore<AnomalyScoreContribution>
    implements AnomalyScoreContributionManager {
  private final ThreatManagementConfigServiceConfig config;
  private final AnomalyScoreContributionConverter anomalyScoreContributionConverter;

  @Inject
  DefaultAnomalyScoreContributionManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      ThreatManagementConfigServiceConfig config,
      AnomalyScoreContributionConverter anomalyScoreContributionConverter) {
    super(
        configServiceBlockingStub,
        THREAT_MANAGEMENT_CONFIG_NAMESPACE,
        ANOMALY_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.config = config;
    this.anomalyScoreContributionConverter = anomalyScoreContributionConverter;
  }

  @Override
  public AnomalyScoreContribution getAnomalyScoreContribution(
      RequestContext requestContext, ScopeConfig scopeConfig) {
    if (scopeConfig.hasEnvironmentScope()) {
      Optional<AnomalyScoreContribution> optionalAnomalyScoreContribution =
          getData(requestContext, scopeConfig.getEnvironmentScope().getEnvironmentId());
      if (optionalAnomalyScoreContribution.isPresent()) {
        return optionalAnomalyScoreContribution.get();
      }
    }
    return getData(requestContext, getTenantId(requestContext))
        .orElseGet(this::getDefaultAnomalyScoreContribution);
  }

  @Override
  public AnomalyScoreContribution upsertAnomalyScoreContribution(
      RequestContext requestContext, AnomalyScoreContribution anomalyScoreContribution) {
    return upsertObject(requestContext, anomalyScoreContribution).getData();
  }

  @Override
  public AnomalyScoreContribution getDefaultAnomalyScoreContribution() {
    return AnomalyScoreContribution.newBuilder()
        .setAnomalyScore(this.config.getDefaultAnomalyContributionScore())
        .build();
  }

  @SneakyThrows
  @Override
  protected Optional<AnomalyScoreContribution> buildDataFromValue(Value value) {
    return anomalyScoreContributionConverter.convert(value);
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(AnomalyScoreContribution anomalyScoreContribution) {
    return anomalyScoreContributionConverter.convert(anomalyScoreContribution);
  }

  @Override
  protected String getContextFromData(AnomalyScoreContribution anomalyScoreContribution) {
    return anomalyScoreContribution.getScope().hasEnvironmentScope()
        ? anomalyScoreContribution.getScope().getEnvironmentScope().getEnvironmentId()
        : getTenantId(RequestContext.CURRENT.get());
  }

  private String getTenantId(RequestContext context) {
    return context
        .getTenantId()
        .orElseThrow(
            () ->
                Status.INVALID_ARGUMENT
                    .withDescription("Unable to get tenant id from request context")
                    .asRuntimeException());
  }
}
