package ai.traceable.threatmanagement.config.service.eventscore;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.SECURITY_EVENT_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;

import ai.traceable.threatmanagement.config.service.ThreatManagementConfigServiceConfig;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventScoreContribution;
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
class DefaultSecurityEventScoreContributionManager
    implements SecurityEventScoreContributionManager {
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final ThreatManagementConfigServiceConfig config;
  private final SecurityEventScoreContributionConverter securityEventScoreContributionConverter;

  @Inject
  DefaultSecurityEventScoreContributionManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ThreatManagementConfigServiceConfig config,
      SecurityEventScoreContributionConverter securityEventScoreContributionConverter) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.config = config;
    this.securityEventScoreContributionConverter = securityEventScoreContributionConverter;
  }

  @Override
  public SecurityEventScoreContribution getSecurityEventScoreContribution(
      RequestContext requestContext) {
    return getSecurityEventScoreContributionConfig(requestContext)
        .orElseGet(
            () ->
                SecurityEventScoreContribution.newBuilder()
                    .setAnomalyScore(this.config.getDefaultSecurityEventContributionAnomalyScore())
                    .setMediumScore(this.config.getDefaultSecurityEventContributionMediumScore())
                    .setHighScore(this.config.getDefaultSecurityEventContributionHighScore())
                    .build());
  }

  @SneakyThrows
  private Optional<SecurityEventScoreContribution> getSecurityEventScoreContributionConfig(
      RequestContext requestContext) {
    String configId = getConfigId(requestContext);
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(SECURITY_EVENT_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME)
            .addContexts(configId)
            .build();

    try {
      Value config =
          requestContext.call(() -> configServiceBlockingStub.getConfig(request).getConfig());

      return securityEventScoreContributionConverter.convert(config);
    } catch (Exception e) {
      if (Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw e;
    }
  }

  @Override
  public SecurityEventScoreContribution upsertSecurityEventScoreContribution(
      RequestContext requestContext,
      SecurityEventScoreContribution securityEventScoreContribution) {
    return upsertSecurityEventScoreContributionConfig(
            requestContext, securityEventScoreContribution)
        .orElseThrow(Status.INTERNAL::asRuntimeException);
  }

  @SneakyThrows
  private Optional<SecurityEventScoreContribution> upsertSecurityEventScoreContributionConfig(
      RequestContext requestContext,
      SecurityEventScoreContribution securityEventScoreContribution) {
    String configId = getConfigId(requestContext);

    UpsertConfigRequest request =
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(SECURITY_EVENT_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME)
            .setConfig(
                securityEventScoreContributionConverter.convert(securityEventScoreContribution))
            .setContext(configId)
            .build();

    UpsertConfigResponse response = configServiceBlockingStub.upsertConfig(request);
    return securityEventScoreContributionConverter.convert(response.getConfig());
  }

  @Override
  public SecurityEventScoreContribution getDefaultSecurityEventScoreContribution() {
    return SecurityEventScoreContribution.newBuilder()
        .setAnomalyScore(this.config.getDefaultSecurityEventContributionAnomalyScore())
        .setMediumScore(this.config.getDefaultSecurityEventContributionMediumScore())
        .setHighScore(this.config.getDefaultSecurityEventContributionHighScore())
        .build();
  }

  private String getConfigId(RequestContext requestContext) {
    // Using tenant id as threat score bound config id, since it's tenant scoped
    return requestContext
        .getTenantId()
        .orElseThrow(
            () -> new IllegalArgumentException("Unable to get config id from request context"));
  }
}
