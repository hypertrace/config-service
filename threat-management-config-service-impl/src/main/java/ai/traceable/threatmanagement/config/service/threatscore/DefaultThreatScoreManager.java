package ai.traceable.threatmanagement.config.service.threatscore;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_SCORE_BOUND_CONFIG_RESOURCE_NAME;

import ai.traceable.threatmanagement.config.service.ThreatManagementConfigServiceConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreBound;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextuallyIdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DefaultThreatScoreManager extends ContextuallyIdentifiedObjectStore<ThreatScoreBound>
    implements ThreatScoreManager {
  private final ThreatManagementConfigServiceConfig config;
  private final ThreatScoreBoundConverter threatScoreBoundConverter;

  @Inject
  DefaultThreatScoreManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      ThreatManagementConfigServiceConfig config,
      ThreatScoreBoundConverter threatScoreBoundConverter) {
    super(
        configServiceBlockingStub,
        THREAT_MANAGEMENT_CONFIG_NAMESPACE,
        THREAT_SCORE_BOUND_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.config = config;
    this.threatScoreBoundConverter = threatScoreBoundConverter;
  }

  @Override
  public ThreatScoreBound getThreatScoreBound(RequestContext requestContext) {
    return getData(requestContext).orElseGet(this::getDefaultThreatScoreBound);
  }

  @Override
  public ThreatScoreBound upsertThreatScoreBound(
      RequestContext requestContext, ThreatScoreBound threatScoreBound) {
    return upsertObject(requestContext, threatScoreBound).getData();
  }

  @Override
  public ThreatScoreBound getDefaultThreatScoreBound() {
    return ThreatScoreBound.newBuilder()
        .setLowScoreUpperBound(this.config.getDefaultThreatUpperBoundLowScore())
        .setMediumScoreUpperBound(this.config.getDefaultThreatUpperBoundMediumScore())
        .setHighScoreUpperBound(this.config.getDefaultThreatUpperBoundHighScore())
        .build();
  }

  @Override
  protected Optional<ThreatScoreBound> buildDataFromValue(Value value) {
    try {
      return threatScoreBoundConverter.convert(value);
    } catch (Exception e) {
      log.error("Unable to build ThreatScoreBound from value : {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ThreatScoreBound data) {
    return threatScoreBoundConverter.convert(data);
  }

  @Override
  protected String getConfigContextFromRequestContext(RequestContext requestContext) {
    return requestContext
        .getTenantId()
        .orElseThrow(
            () ->
                Status.INVALID_ARGUMENT
                    .withDescription("Unable to get tenant id from request context")
                    .asRuntimeException());
  }
}
