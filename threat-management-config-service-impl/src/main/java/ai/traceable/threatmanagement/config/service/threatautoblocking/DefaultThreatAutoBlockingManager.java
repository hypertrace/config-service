package ai.traceable.threatmanagement.config.service.threatautoblocking;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_AUTO_BLOCKING_CONFIG_RESOURCE_NAME;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;

import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionConfig;
import ai.traceable.threatmanagement.config.service.v1.ThreatAutoBlockingActionType;
import ai.traceable.threatmanagement.config.service.v1.UpdateThreatAutoBlockingConfigRequest;
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
class DefaultThreatAutoBlockingManager implements ThreatAutoBlockingManager {
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final ThreatAutoBlockingActionConfigConverter threatAutoBlockingActionConfigConverter;
  private final ThreatAutoBlockingConverter threatAutoBlockingConverter;

  @Inject
  DefaultThreatAutoBlockingManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ThreatAutoBlockingActionConfigConverter threatAutoBlockingActionConfigConverter,
      ThreatAutoBlockingConverter threatAutoBlockingConverter) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.threatAutoBlockingActionConfigConverter = threatAutoBlockingActionConfigConverter;
    this.threatAutoBlockingConverter = threatAutoBlockingConverter;
  }

  @Override
  public ThreatAutoBlockingActionConfig getThreatAutoBlockingAction(RequestContext requestContext) {
    return getThreatAutoBlockingActionConfig(requestContext)
        .orElseGet(this::getDefaultThreatAutoBlockingActionConfig);
  }

  @SneakyThrows
  private Optional<ThreatAutoBlockingActionConfig> getThreatAutoBlockingActionConfig(
      RequestContext requestContext) {
    String configId = getConfigId(requestContext);
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(THREAT_AUTO_BLOCKING_CONFIG_RESOURCE_NAME)
            .addContexts(configId)
            .build();

    try {
      Value config =
          requestContext.call(() -> configServiceBlockingStub.getConfig(request).getConfig());

      return threatAutoBlockingConverter.convert(config);
    } catch (Exception e) {
      if (Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw e;
    }
  }

  @Override
  public ThreatAutoBlockingActionConfig upsertThreatAutoBlockingAction(
      RequestContext requestContext, UpdateThreatAutoBlockingConfigRequest request) {
    return upsertThreatAutoBlockingConfig(requestContext, request)
        .orElseThrow(Status.INTERNAL::asRuntimeException);
  }

  @SneakyThrows
  private Optional<ThreatAutoBlockingActionConfig> upsertThreatAutoBlockingConfig(
      RequestContext requestContext, UpdateThreatAutoBlockingConfigRequest request) {
    String configId = getConfigId(requestContext);
    ThreatAutoBlockingActionConfig threatAutoBlockingActionConfig =
        threatAutoBlockingActionConfigConverter.convert(request);
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(THREAT_AUTO_BLOCKING_CONFIG_RESOURCE_NAME)
            .setConfig(threatAutoBlockingConverter.convert(threatAutoBlockingActionConfig))
            .setContext(configId)
            .build();

    UpsertConfigResponse response = configServiceBlockingStub.upsertConfig(upsertConfigRequest);
    return threatAutoBlockingConverter.convert(response.getConfig());
  }

  private ThreatAutoBlockingActionConfig getDefaultThreatAutoBlockingActionConfig() {
    return ThreatAutoBlockingActionConfig.newBuilder()
        .setActionType(ThreatAutoBlockingActionType.THREAT_AUTO_BLOCKING_ACTION_TYPE_NO_ACTION)
        .build();
  }

  private String getConfigId(RequestContext requestContext) {
    // Using tenant id as threat auto blocking action config id, since it's tenant scoped
    return requestContext
        .getTenantId()
        .orElseThrow(
            () -> new IllegalArgumentException("Unable to get config id from request context"));
  }
}
