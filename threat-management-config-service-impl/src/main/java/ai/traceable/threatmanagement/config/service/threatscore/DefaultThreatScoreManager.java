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
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DefaultThreatScoreManager implements ThreatScoreManager {
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final ThreatManagementConfigServiceConfig config;
  private final ThreatScoreBoundConverter threatScoreBoundConverter;

  @Inject
  DefaultThreatScoreManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ThreatManagementConfigServiceConfig config,
      ThreatScoreBoundConverter threatScoreBoundConverter) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.config = config;
    this.threatScoreBoundConverter = threatScoreBoundConverter;
  }

  @Override
  public ThreatScoreBound getThreatScoreBound(RequestContext requestContext) {
    return getThreatScoreBoundConfig(requestContext)
        .orElseGet(
            () ->
                ThreatScoreBound.newBuilder()
                    .setMediumScoreUpperBound(this.config.getDefaultThreatUpperBoundMediumScore())
                    .setHighScoreUpperBound(this.config.getDefaultThreatUpperBoundHighScore())
                    .build());
  }

  @SneakyThrows
  private Optional<ThreatScoreBound> getThreatScoreBoundConfig(RequestContext requestContext) {
    String configId = getConfigId(requestContext);
    GetConfigRequest getThreatScoreBoundRequest =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(THREAT_SCORE_BOUND_CONFIG_RESOURCE_NAME)
            .addContexts(configId)
            .build();

    try {
      Value config =
          requestContext.call(
              () -> configServiceBlockingStub.getConfig(getThreatScoreBoundRequest).getConfig());
      return threatScoreBoundConverter.convert(config);
    } catch (Exception e) {
      if (Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      log.error("Unable to get threat score bound config");
      throw e;
    }
  }

  @Override
  public ThreatScoreBound upsertThreatScoreBound(
      RequestContext requestContext, ThreatScoreBound threatScoreBound) {
    return upsertThreatScoreBoundConfig(requestContext, threatScoreBound)
        .orElseThrow(Status.INTERNAL::asRuntimeException);
  }

  @SneakyThrows
  private Optional<ThreatScoreBound> upsertThreatScoreBoundConfig(
      RequestContext requestContext, ThreatScoreBound threatScoreBound) {
    String configId = getConfigId(requestContext);

    try {
      UpsertConfigRequest request =
          UpsertConfigRequest.newBuilder()
              .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
              .setResourceName(THREAT_SCORE_BOUND_CONFIG_RESOURCE_NAME)
              .setConfig(threatScoreBoundConverter.convert(threatScoreBound))
              .setContext(configId)
              .build();

      UpsertConfigResponse response = configServiceBlockingStub.upsertConfig(request);
      return threatScoreBoundConverter.convert(response.getConfig());
    } catch (Exception e) {
      log.error("Unable to upsert threat score bound config {}", threatScoreBound);
      throw e;
    }
  }

  private String getConfigId(RequestContext requestContext) {
    // Using tenant id as threat score bound config id, since it's tenant scoped
    return requestContext
        .getTenantId()
        .orElseThrow(
            () -> new IllegalArgumentException("Unable to get config id from request context"));
  }
}
