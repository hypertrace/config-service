package ai.traceable.threatmanagement.config.service.threatscore;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_SCORE_DECAY_CONFIG_RESOURCE_NAME;

import ai.traceable.threatmanagement.config.service.ThreatManagementConfigServiceConfig;
import ai.traceable.threatmanagement.config.service.v1.DecayValue;
import ai.traceable.threatmanagement.config.service.v1.ScopeConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatScoreDecay;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.time.Duration;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DefaultThreatScoreDecayManager extends IdentifiedObjectStore<ThreatScoreDecay>
    implements ThreatScoreDecayManager {
  private final ThreatManagementConfigServiceConfig config;
  private final ThreatScoreDecayConverter threatScoreDecayConverter;

  @Inject
  DefaultThreatScoreDecayManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      ThreatManagementConfigServiceConfig config,
      ThreatScoreDecayConverter threatScoreDecayConverter) {
    super(
        configServiceBlockingStub,
        THREAT_MANAGEMENT_CONFIG_NAMESPACE,
        THREAT_SCORE_DECAY_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.config = config;
    this.threatScoreDecayConverter = threatScoreDecayConverter;
  }

  @Override
  public ThreatScoreDecay getThreatScoreDecay(
      RequestContext requestContext, ScopeConfig scopeConfig) {
    if (scopeConfig.hasEnvironmentScope()) {
      Optional<ThreatScoreDecay> optionalThreatScoreDecayConfig =
          getData(requestContext, scopeConfig.getEnvironmentScope().getEnvironmentId());
      if (optionalThreatScoreDecayConfig.isPresent()) {
        return optionalThreatScoreDecayConfig.get();
      }
    }
    return getData(requestContext, getTenantId(requestContext))
        .orElseGet(this::getDefaultThreatScoreDecay);
  }

  @Override
  public ThreatScoreDecay upsertThreatScoreDecay(
      RequestContext requestContext, ThreatScoreDecay threatScoreDecay) {
    return upsertObject(requestContext, threatScoreDecay).getData();
  }

  @Override
  public ThreatScoreDecay getDefaultThreatScoreDecay() {

    Duration defaultThreatDecayAfterDuration = this.config.getDefaultThreatDecayAfterDuration();
    com.google.protobuf.Duration protoDuration =
        com.google.protobuf.Duration.newBuilder()
            .setSeconds(defaultThreatDecayAfterDuration.getSeconds())
            .setNanos(defaultThreatDecayAfterDuration.getNano())
            .build();

    return ThreatScoreDecay.newBuilder()
        .setDecayAfterDuration(protoDuration)
        .setDecayValue(
            DecayValue.newBuilder()
                .setPercentageDecayValue(this.config.getDefaultThreatDecayPercentageValue())
                .build())
        .build();
  }

  @Override
  protected Optional<ThreatScoreDecay> buildDataFromValue(Value value) {
    try {
      return threatScoreDecayConverter.convert(value);
    } catch (Exception e) {
      log.error("Unable to build ThreatScoreDecay from value : {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ThreatScoreDecay data) {
    return threatScoreDecayConverter.convert(data);
  }

  @Override
  protected String getContextFromData(ThreatScoreDecay threatScoreDecay) {
    return threatScoreDecay.getScope().hasEnvironmentScope()
        ? threatScoreDecay.getScope().getEnvironmentScope().getEnvironmentId()
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
