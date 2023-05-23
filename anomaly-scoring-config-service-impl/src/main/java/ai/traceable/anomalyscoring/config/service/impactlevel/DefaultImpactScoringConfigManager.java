package ai.traceable.anomalyscoring.config.service.impactlevel;

import static ai.traceable.anomalyscoring.config.service.constants.AnomalyScoringConfigConstants.ANOMALY_SCORING_CONFIG_NAMESPACE;
import static ai.traceable.anomalyscoring.config.service.constants.AnomalyScoringConfigConstants.IMPACT_CONFIG_RESOURCE_NAME;

import ai.traceable.anomalyscoring.config.service.AnomalyScoringConfigServiceConfig;
import ai.traceable.anomalyscoring.config.service.v1.ImpactScoreLevelConfig;
import ai.traceable.anomalyscoring.config.service.v1.ImpactScoringConfig;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextuallyIdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DefaultImpactScoringConfigManager
    extends ContextuallyIdentifiedObjectStore<ImpactScoringConfig>
    implements ImpactScoringConfigManager {
  private final AnomalyScoringConfigServiceConfig config;
  private final ImpactScoringConfigConverter impactScoringConfigConverter;

  @Inject
  DefaultImpactScoringConfigManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      AnomalyScoringConfigServiceConfig config,
      ImpactScoringConfigConverter impactScoringConfigConverter) {
    super(
        configServiceBlockingStub,
        ANOMALY_SCORING_CONFIG_NAMESPACE,
        IMPACT_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.config = config;
    this.impactScoringConfigConverter = impactScoringConfigConverter;
  }

  @Override
  public ImpactScoringConfig getImpactScoringConfig(RequestContext requestContext) {
    return getData(requestContext).orElseGet(this::getDefaultImpactScoringConfig);
  }

  @Override
  public ImpactScoringConfig upsertImpactScoringConfig(
      RequestContext requestContext, ImpactScoringConfig impactScoringConfig) {
    return upsertObject(requestContext, impactScoringConfig).getData();
  }

  @Override
  public ImpactScoringConfig getDefaultImpactScoringConfig() {
    ImpactScoreLevelConfig impactScoreLevelConfig =
        ImpactScoreLevelConfig.newBuilder()
            .setMediumLevelMinScore(this.config.getDefaultMediumImpactMinScore())
            .setHighLevelMinScore(this.config.getDefaultHighImpactMinScore())
            .build();
    return ImpactScoringConfig.newBuilder()
        .setImpactScoreLevelConfig(impactScoreLevelConfig)
        .build();
  }

  @SneakyThrows
  @Override
  protected Optional<ImpactScoringConfig> buildDataFromValue(Value value) {
    return impactScoringConfigConverter.convert(value);
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ImpactScoringConfig impactScoringConfig) {
    return impactScoringConfigConverter.convert(impactScoringConfig);
  }

  @Override
  protected String getConfigContextFromRequestContext(RequestContext requestContext) {
    return requestContext
        .getTenantId()
        .orElseThrow(
            () -> new IllegalArgumentException("Unable to get config id from request context"));
  }
}
