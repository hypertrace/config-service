package ai.traceable.threatmanagement.config.service.ipreputation;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.IP_REPUTATION_THREAT_SCORE_CONFIG_RESOURCE_NAME;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;

import ai.traceable.threatmanagement.config.service.ThreatManagementConfigServiceConfig;
import ai.traceable.threatmanagement.config.service.v1.IpReputationThreatScoreConfig;
import ai.traceable.threatmanagement.config.service.v1.ScopeConfig;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class IpReputationThreatScoreConfigManagerImpl
    extends IdentifiedObjectStore<IpReputationThreatScoreConfig>
    implements IpReputationThreatScoreConfigManager {

  private final ThreatManagementConfigServiceConfig config;

  @Inject
  IpReputationThreatScoreConfigManagerImpl(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      ThreatManagementConfigServiceConfig config) {
    super(
        configServiceBlockingStub,
        THREAT_MANAGEMENT_CONFIG_NAMESPACE,
        IP_REPUTATION_THREAT_SCORE_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
    this.config = config;
  }

  @Override
  public IpReputationThreatScoreConfig getIpReputationThreatScoreConfig(
      RequestContext requestContext, ScopeConfig scopeConfig) {
    String contextId =
        scopeConfig.hasEnvironmentScope()
            ? scopeConfig.getEnvironmentScope().getEnvironmentId()
            : getTenantId(requestContext);
    return getData(requestContext, contextId).orElse(getDefaultIpReputationThreatScoreConfig());
  }

  @Override
  public IpReputationThreatScoreConfig updateIpReputationThreatScoreConfig(
      RequestContext requestContext, IpReputationThreatScoreConfig config) {
    return upsertObject(requestContext, config).getData();
  }

  @Override
  public IpReputationThreatScoreConfig getDefaultIpReputationThreatScoreConfig() {
    return IpReputationThreatScoreConfig.newBuilder()
        .setCriticalIpReputationThreatScoreIncrement(
            config.getDefaultCriticalIpReputationThreatScoreIncrement())
        .setHighIpReputationThreatScoreIncrement(
            config.getDefaultHighIpReputationThreatScoreIncrement())
        .setMediumIpReputationThreatScoreIncrement(
            config.getDefaultMediumIpReputationThreatScoreIncrement())
        .setLowIpReputationThreatScoreIncrement(
            config.getDefaultLowIpReputationThreatScoreIncrement())
        .build();
  }

  @Override
  protected Optional<IpReputationThreatScoreConfig> buildDataFromValue(Value value) {
    try {
      return IpReputationThreatScoreConfigConverter.convert(value);
    } catch (InvalidProtocolBufferException e) {
      log.error("Unable to convert Value:{} to IpReputationThreatScoreConfig", value, e);
      return Optional.empty();
    }
  }

  @Override
  protected Value buildValueFromData(IpReputationThreatScoreConfig data) {
    try {
      return IpReputationThreatScoreConfigConverter.convert(data);
    } catch (InvalidProtocolBufferException e) {
      log.error("Unable to convert IpReputationThreatScoreConfig:{} to Value", data, e);
      return Value.getDefaultInstance();
    }
  }

  @Override
  protected String getContextFromData(IpReputationThreatScoreConfig data) {
    return data.getScope().hasEnvironmentScope()
        ? data.getScope().getEnvironmentScope().getEnvironmentId()
        : getTenantId(RequestContext.CURRENT.get());
  }

  private String getTenantId(RequestContext requestContext) {
    return requestContext
        .getTenantId()
        .orElseThrow(
            () -> new IllegalArgumentException("Unable to get tenant id from request context"));
  }
}
