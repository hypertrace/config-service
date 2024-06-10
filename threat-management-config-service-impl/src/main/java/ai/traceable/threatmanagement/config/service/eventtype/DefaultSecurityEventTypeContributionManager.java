package ai.traceable.threatmanagement.config.service.eventtype;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.SECURITY_EVENT_TYPE_CONTRIBUTION_CONFIG_RESOURCE_NAME;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;

import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution.SecurityEventTypeContributionKind;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class DefaultSecurityEventTypeContributionManager implements SecurityEventTypeContributionManager {
  private final ConfigServiceBlockingStub configServiceBlockingStub;
  private final SecurityEventTypeContributionConverter securityEventTypeContributionConverter;
  private final ClientConfig clientConfig;

  @Inject
  DefaultSecurityEventTypeContributionManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      SecurityEventTypeContributionConverter securityEventTypeContributionConverter,
      ClientConfig clientConfig) {
    this.configServiceBlockingStub = configServiceBlockingStub;
    this.securityEventTypeContributionConverter = securityEventTypeContributionConverter;
    this.clientConfig = clientConfig;
  }

  @Override
  public SecurityEventTypeContribution getSecurityEventTypeContribution(
      RequestContext requestContext) {
    return getSecurityEventTypeContributionConfig(requestContext)
        .orElseGet(this::getDefaultSecurityEventTypeContribution);
  }

  @SneakyThrows
  private Optional<SecurityEventTypeContribution> getSecurityEventTypeContributionConfig(
      RequestContext requestContext) {
    String configId = getConfigId(requestContext);
    GetConfigRequest request =
        GetConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(SECURITY_EVENT_TYPE_CONTRIBUTION_CONFIG_RESOURCE_NAME)
            .addContexts(configId)
            .build();

    try {
      Value config =
          requestContext.call(
              () ->
                  configServiceBlockingStub
                      .withDeadlineAfter(
                          clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                      .getConfig(request)
                      .getConfig());

      return securityEventTypeContributionConverter.convert(config);
    } catch (Exception e) {
      if (Status.fromThrowable(e).equals(Status.NOT_FOUND)) {
        return Optional.empty();
      }
      throw e;
    }
  }

  @Override
  public SecurityEventTypeContribution upsertSecurityEventTypeContribution(
      RequestContext requestContext, SecurityEventTypeContribution securityEventTypeContribution) {
    return upsertSecurityEventTypeContributionConfig(requestContext, securityEventTypeContribution)
        .orElseThrow(Status.INTERNAL::asRuntimeException);
  }

  @SneakyThrows
  private Optional<SecurityEventTypeContribution> upsertSecurityEventTypeContributionConfig(
      RequestContext requestContext, SecurityEventTypeContribution securityEventTypeContribution) {
    String configId = getConfigId(requestContext);

    UpsertConfigRequest request =
        UpsertConfigRequest.newBuilder()
            .setResourceNamespace(THREAT_MANAGEMENT_CONFIG_NAMESPACE)
            .setResourceName(SECURITY_EVENT_TYPE_CONTRIBUTION_CONFIG_RESOURCE_NAME)
            .setConfig(
                securityEventTypeContributionConverter.convert(securityEventTypeContribution))
            .setContext(configId)
            .build();

    UpsertConfigResponse response =
        configServiceBlockingStub
            .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
            .upsertConfig(request);
    return securityEventTypeContributionConverter.convert(response.getConfig());
  }

  private SecurityEventTypeContribution getDefaultSecurityEventTypeContribution() {
    return SecurityEventTypeContribution.newBuilder()
        .setSecurityEventTypeContributionKind(
            SecurityEventTypeContributionKind.SECURITY_EVENT_TYPE_CONTRIBUTION_KIND_ALL)
        .build();
  }

  private String getConfigId(RequestContext requestContext) {
    // Using tenant id as security event type contribution config id, since it's tenant scoped
    return requestContext
        .getTenantId()
        .orElseThrow(
            () -> new IllegalArgumentException("Unable to get config id from request context"));
  }
}
