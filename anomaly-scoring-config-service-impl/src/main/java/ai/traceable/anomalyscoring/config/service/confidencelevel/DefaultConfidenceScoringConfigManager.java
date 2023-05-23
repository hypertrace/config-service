package ai.traceable.anomalyscoring.config.service.confidencelevel;

import static ai.traceable.anomalyscoring.config.service.constants.AnomalyScoringConfigConstants.ANOMALY_SCORING_CONFIG_NAMESPACE;
import static ai.traceable.anomalyscoring.config.service.constants.AnomalyScoringConfigConstants.CONFIDENCE_CONFIG_RESOURCE_NAME;

import ai.traceable.anomalyscoring.config.service.AnomalyScoringConfigServiceConfig;
import ai.traceable.anomalyscoring.config.service.v1.ConfidenceScoreLevelConfig;
import ai.traceable.anomalyscoring.config.service.v1.ConfidenceScoringConfig;
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
class DefaultConfidenceScoringConfigManager
    extends ContextuallyIdentifiedObjectStore<ConfidenceScoringConfig>
    implements ConfidenceScoringConfigManager {
  private final AnomalyScoringConfigServiceConfig config;
  private final ConfidenceScoringConfigConverter confidenceScoringConfigConverter;

  @Inject
  DefaultConfidenceScoringConfigManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      AnomalyScoringConfigServiceConfig config,
      ConfidenceScoringConfigConverter confidenceScoringConfigConverter) {
    super(
        configServiceBlockingStub,
        ANOMALY_SCORING_CONFIG_NAMESPACE,
        CONFIDENCE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.config = config;
    this.confidenceScoringConfigConverter = confidenceScoringConfigConverter;
  }

  @Override
  public ConfidenceScoringConfig getConfidenceScoringConfig(RequestContext requestContext) {
    return getData(requestContext).orElseGet(this::getDefaultConfidenceScoringConfig);
  }

  public ConfidenceScoringConfig upsertConfidenceScoringConfig(
      RequestContext requestContext, ConfidenceScoringConfig confidenceScoringConfig) {
    return upsertObject(requestContext, confidenceScoringConfig).getData();
  }

  @Override
  public ConfidenceScoringConfig getDefaultConfidenceScoringConfig() {
    ConfidenceScoreLevelConfig confidenceScoreLevelConfig =
        ConfidenceScoreLevelConfig.newBuilder()
            .setMediumLevelMinScore(this.config.getDefaultMediumConfidenceMinScore())
            .setHighLevelMinScore(this.config.getDefaultHighConfidenceMinScore())
            .build();
    return ConfidenceScoringConfig.newBuilder()
        .setConfidenceScoreLevelConfig(confidenceScoreLevelConfig)
        .build();
  }

  @SneakyThrows
  @Override
  protected Optional<ConfidenceScoringConfig> buildDataFromValue(Value value) {
    return confidenceScoringConfigConverter.convert(value);
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ConfidenceScoringConfig confidenceScoringConfig) {
    return confidenceScoringConfigConverter.convert(confidenceScoringConfig);
  }

  @Override
  protected String getConfigContextFromRequestContext(RequestContext requestContext) {
    return requestContext
        .getTenantId()
        .orElseThrow(
            () -> new IllegalArgumentException("Unable to get config id from request context"));
  }
}
