package ai.traceable.threatmanagement.config.service.threatautoblocking;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_AUTO_BLOCKING_CONFIG_RESOURCE_NAME;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;

import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionType;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest;
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
class DefaultThreatAutoBlockingManager
    extends ContextuallyIdentifiedObjectStore<ThreatAutoBlockingActionConfig>
    implements ThreatAutoBlockingManager {
  private final ThreatAutoBlockingActionConfigConverter threatAutoBlockingActionConfigConverter;
  private final ThreatAutoBlockingConverter threatAutoBlockingConverter;

  @Inject
  DefaultThreatAutoBlockingManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      ThreatAutoBlockingActionConfigConverter threatAutoBlockingActionConfigConverter,
      ThreatAutoBlockingConverter threatAutoBlockingConverter) {
    super(
        configServiceBlockingStub,
        THREAT_MANAGEMENT_CONFIG_NAMESPACE,
        THREAT_AUTO_BLOCKING_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.threatAutoBlockingActionConfigConverter = threatAutoBlockingActionConfigConverter;
    this.threatAutoBlockingConverter = threatAutoBlockingConverter;
  }

  @Override
  public ThreatAutoBlockingActionConfig getThreatAutoBlockingAction(RequestContext requestContext) {
    return getData(requestContext).orElseGet(this::getDefaultThreatAutoBlockingActionConfig);
  }

  @Override
  public ThreatAutoBlockingActionConfig upsertThreatAutoBlockingAction(
      RequestContext requestContext, UpdateThreatAutoBlockingConfigRequest request) {
    return upsertObject(requestContext, threatAutoBlockingActionConfigConverter.convert(request))
        .getData();
  }

  private ThreatAutoBlockingActionConfig getDefaultThreatAutoBlockingActionConfig() {
    return ThreatAutoBlockingActionConfig.newBuilder()
        .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_NO_ACTION)
        .build();
  }

  @SneakyThrows
  @Override
  protected Optional<ThreatAutoBlockingActionConfig> buildDataFromValue(Value value) {
    return threatAutoBlockingConverter.convert(value);
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(
      ThreatAutoBlockingActionConfig threatAutoBlockingActionConfig) {
    return threatAutoBlockingConverter.convert(threatAutoBlockingActionConfig);
  }

  @Override
  protected String getConfigContextFromRequestContext(RequestContext requestContext) {
    // Using tenant id as threat auto blocking action config id, since it's tenant scoped
    return requestContext
        .getTenantId()
        .orElseThrow(
            () -> new IllegalArgumentException("Unable to get config id from request context"));
  }
}
