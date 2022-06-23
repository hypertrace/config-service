package ai.traceable.threatmanagement.config.service.statuscode;

import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.STATUS_CODE_THREAT_SCORE_CONFIGS_RESOURCE_NAME;
import static ai.traceable.threatmanagement.config.service.constants.ThreatManagementConfigConstants.THREAT_MANAGEMENT_CONFIG_NAMESPACE;

import ai.traceable.threatmanagement.config.service.v1.StatusCodeThreatScoreConfigs;
import com.google.inject.Inject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
class StatusCodeThreatScoreConfigsManagerImpl
    extends IdentifiedObjectStore<StatusCodeThreatScoreConfigs>
    implements StatusCodeThreatScoreConfigsManager {

  @Inject
  StatusCodeThreatScoreConfigsManagerImpl(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub) {
    super(
        configServiceBlockingStub,
        THREAT_MANAGEMENT_CONFIG_NAMESPACE,
        STATUS_CODE_THREAT_SCORE_CONFIGS_RESOURCE_NAME);
  }

  @Override
  public StatusCodeThreatScoreConfigs getStatusCodeThreatScoreConfigs(
      RequestContext requestContext) {
    return getData(requestContext, getTenantId())
        .orElse(StatusCodeThreatScoreConfigs.getDefaultInstance());
  }

  @Override
  public StatusCodeThreatScoreConfigs updateStatusCodeThreatScoreConfigs(
      RequestContext requestContext, StatusCodeThreatScoreConfigs configs) {
    return upsertObject(requestContext, configs).getData();
  }

  @Override
  protected Optional<StatusCodeThreatScoreConfigs> buildDataFromValue(Value value) {
    try {
      return StatusCodeThreatScoreConfigsConverter.convert(value);
    } catch (InvalidProtocolBufferException e) {
      log.error("Unable to convert Value:{} to StatusCodeThreatScoreConfigs", value, e);
      return Optional.empty();
    }
  }

  @Override
  protected Value buildValueFromData(StatusCodeThreatScoreConfigs data) {
    try {
      return StatusCodeThreatScoreConfigsConverter.convert(data);
    } catch (InvalidProtocolBufferException e) {
      log.error("Unable to convert StatusCodeThreatScoreConfigs:{} to Value", data, e);
      return Value.getDefaultInstance();
    }
  }

  @Override
  protected String getContextFromData(StatusCodeThreatScoreConfigs data) {
    return getTenantId();
  }

  private String getTenantId() {
    return RequestContext.CURRENT
        .get()
        .getTenantId()
        .orElseThrow(
            () -> new IllegalArgumentException("Unable to get tenant id from request context"));
  }
}
