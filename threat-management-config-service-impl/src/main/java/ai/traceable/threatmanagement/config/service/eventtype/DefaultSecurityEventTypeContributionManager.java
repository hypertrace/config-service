package ai.traceable.threatmanagement.config.service.eventtype;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.SECURITY_EVENT_TYPE_CONTRIBUTION_CONFIG_RESOURCE_NAME;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;

import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution;
import ai.traceable.threatmanagement.config.service.v1.SecurityEventTypeContribution.SecurityEventTypeContributionKind;
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
class DefaultSecurityEventTypeContributionManager
    extends ContextuallyIdentifiedObjectStore<SecurityEventTypeContribution>
    implements SecurityEventTypeContributionManager {
  private final SecurityEventTypeContributionConverter securityEventTypeContributionConverter;

  @Inject
  DefaultSecurityEventTypeContributionManager(
      ConfigServiceBlockingStub configServiceBlockingStub,
      SecurityEventTypeContributionConverter securityEventTypeContributionConverter,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        THREAT_MANAGEMENT_CONFIG_NAMESPACE,
        SECURITY_EVENT_TYPE_CONTRIBUTION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.securityEventTypeContributionConverter = securityEventTypeContributionConverter;
  }

  @Override
  public SecurityEventTypeContribution getSecurityEventTypeContribution(
      RequestContext requestContext) {
    return getData(requestContext).orElseGet(this::getDefaultSecurityEventTypeContribution);
  }

  @Override
  public SecurityEventTypeContribution upsertSecurityEventTypeContribution(
      RequestContext requestContext, SecurityEventTypeContribution securityEventTypeContribution) {
    return upsertObject(requestContext, securityEventTypeContribution).getData();
  }

  private SecurityEventTypeContribution getDefaultSecurityEventTypeContribution() {
    return SecurityEventTypeContribution.newBuilder()
        .setSecurityEventTypeContributionKind(
            SecurityEventTypeContributionKind.SECURITY_EVENT_TYPE_CONTRIBUTION_KIND_ALL)
        .build();
  }

  @Override
  protected Optional<SecurityEventTypeContribution> buildDataFromValue(Value value) {
    try {
      return securityEventTypeContributionConverter.convert(value);
    } catch (Exception e) {
      log.error("Unable to build SecurityEventTypeContribution from value : {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(SecurityEventTypeContribution data) {
    return securityEventTypeContributionConverter.convert(data);
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
