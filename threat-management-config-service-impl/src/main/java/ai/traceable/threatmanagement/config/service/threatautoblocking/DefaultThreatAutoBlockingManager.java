package ai.traceable.threatmanagement.config.service.threatautoblocking;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_AUTO_BLOCKING_CONFIG_RESOURCE_NAME;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;

import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionType;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextuallyIdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

@Slf4j
class DefaultThreatAutoBlockingManager
    extends ContextuallyIdentifiedObjectStore<ThreatAutoBlockingActionConfig>
    implements ThreatAutoBlockingManager {

  private static final String THREAT_AUTO_BLOCKING_ACTION_CONFIG_TIMER =
      "config.threat.autoblocking.action.timer";
  private static final String TENANT_ID_TAG = "tenantId";
  private static final String OPERATION_TAG = "operation";
  private static final String ENABLED_TAG_VALUE = "enabled";
  private static final String DISABLED_TAG_VALUE = "disabled";
  private static final String DEFAULT_UPDATED_TAG_VALUE = "updated";
  private static final Map<Tags, Timer> TIMER_MAP = new ConcurrentHashMap<>();

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
    ThreatAutoBlockingActionConfig existingConfig = getThreatAutoBlockingAction(requestContext);
    String tenantId = requestContext.getTenantId().get();

    String operation;
    if (!existingConfig.getActionType().equals(request.getActionType())) {
      operation =
          ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_BLOCK.equals(
                  request.getActionType())
              ? ENABLED_TAG_VALUE
              : DISABLED_TAG_VALUE;
    } else {
      operation = DEFAULT_UPDATED_TAG_VALUE;
    }

    return getTimer(tenantId, operation)
        .record(
            () -> {
              ThreatAutoBlockingActionConfig upsertedConfig =
                  upsertObject(
                          requestContext, threatAutoBlockingActionConfigConverter.convert(request))
                      .getData();
              log.info(
                  "Threat AUTO-BLOCKING has been {} by user-email:{} in tenant:{}",
                  operation,
                  requestContext.getEmail().orElse("UNKNOWN"),
                  tenantId);
              return upsertedConfig;
            });
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

  private Timer getTimer(String tenantId, String operation) {
    Tags metricTags = Tags.of(TENANT_ID_TAG, tenantId, OPERATION_TAG, operation);
    return TIMER_MAP.computeIfAbsent(
        metricTags,
        id ->
            PlatformMetricsRegistry.registerTimer(
                THREAT_AUTO_BLOCKING_ACTION_CONFIG_TIMER,
                metricTags.stream().collect(Collectors.toMap(Tag::getKey, Tag::getValue))));
  }
}
