package ai.traceable.threatmanagement.config.service.eventscore;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.SECURITY_EVENT_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;

import ai.traceable.threatmanagement.config.service.ThreatManagementConfigServiceConfig;
import ai.traceable.threatmanagement.config.service.v1.ScopeConfig;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventScoreContribution;
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
class DefaultSecurityEventScoreContributionManager
    extends IdentifiedObjectStore<SecurityEventScoreContribution>
    implements SecurityEventScoreContributionManager {
  private final ThreatManagementConfigServiceConfig config;
  private final SecurityEventScoreContributionConverter securityEventScoreContributionConverter;

  @Inject
  DefaultSecurityEventScoreContributionManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      ThreatManagementConfigServiceConfig config,
      SecurityEventScoreContributionConverter securityEventScoreContributionConverter) {
    super(
        configServiceBlockingStub,
        THREAT_MANAGEMENT_CONFIG_NAMESPACE,
        SECURITY_EVENT_SCORE_CONTRIBUTION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.config = config;
    this.securityEventScoreContributionConverter = securityEventScoreContributionConverter;
  }

  @Override
  public SecurityEventScoreContribution getSecurityEventScoreContribution(
      RequestContext requestContext, ScopeConfig scopeConfig) {
    String contextId =
        scopeConfig.hasEnvironmentScope()
            ? scopeConfig.getEnvironmentScope().getEnvironmentId()
            : getTenantId(requestContext);
    return getData(requestContext, contextId)
        .orElseGet(this::getDefaultSecurityEventScoreContribution);
  }

  @Override
  public SecurityEventScoreContribution upsertSecurityEventScoreContribution(
      RequestContext requestContext,
      SecurityEventScoreContribution securityEventScoreContribution) {
    return upsertObject(requestContext, securityEventScoreContribution).getData();
  }

  @Override
  public SecurityEventScoreContribution getDefaultSecurityEventScoreContribution() {
    return SecurityEventScoreContribution.newBuilder()
        .setLowScore(this.config.getDefaultSecurityEventContributionLowScore())
        .setMediumScore(this.config.getDefaultSecurityEventContributionMediumScore())
        .setHighScore(this.config.getDefaultSecurityEventContributionHighScore())
        .setCriticalScore(this.config.getDefaultSecurityEventContributionCriticalScore())
        .build();
  }

  @SneakyThrows
  @Override
  protected Optional<SecurityEventScoreContribution> buildDataFromValue(Value value) {
    return securityEventScoreContributionConverter.convert(value);
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(
      SecurityEventScoreContribution securityEventScoreContribution) {
    return securityEventScoreContributionConverter.convert(securityEventScoreContribution);
  }

  @Override
  protected String getContextFromData(
      SecurityEventScoreContribution securityEventScoreContribution) {
    return securityEventScoreContribution.getScope().hasEnvironmentScope()
        ? securityEventScoreContribution.getScope().getEnvironmentScope().getEnvironmentId()
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
