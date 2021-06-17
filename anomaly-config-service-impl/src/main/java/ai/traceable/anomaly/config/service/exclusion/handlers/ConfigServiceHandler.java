package ai.traceable.anomaly.config.service.exclusion.handlers;

import static ai.traceable.anomaly.config.service.exclusion.AnomalyExclusionConfigServiceConstants.ANOMALY_EXCLUSION_CONFIG_NAMESPACE;
import static ai.traceable.anomaly.config.service.exclusion.AnomalyExclusionConfigServiceConstants.ANOMALY_EXCLUSION_CONFIG_RESOURCE_NAME;

import ai.traceable.anomaly.config.service.exclusion.AnomalyExclusionConfigServiceConstants;
import com.google.protobuf.Value;
import javax.inject.Inject;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.DeleteConfigRequest;
import org.hypertrace.config.service.v1.DeleteConfigResponse;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.GetAllConfigsResponse;
import org.hypertrace.config.service.v1.GetConfigRequest;
import org.hypertrace.config.service.v1.GetConfigResponse;
import org.hypertrace.config.service.v1.UpsertConfigRequest;
import org.hypertrace.config.service.v1.UpsertConfigResponse;

public class ConfigServiceHandler {
  private final ConfigServiceBlockingStub configServiceBlockingStub;

  @Inject
  ConfigServiceHandler(ConfigServiceBlockingStub configServiceBlockingStub) {
    this.configServiceBlockingStub = configServiceBlockingStub;
  }

  public GetConfigResponse getExclusionConfigByRuleId(String ruleId) {
    GetConfigRequest getConfigRequest =
        GetConfigRequest.newBuilder()
            .addContexts(ruleId)
            .setResourceName(ANOMALY_EXCLUSION_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(ANOMALY_EXCLUSION_CONFIG_NAMESPACE)
            .build();

    return configServiceBlockingStub.getConfig(getConfigRequest);
  }

  public GetAllConfigsResponse getAllExclusionConfigs() {
    GetAllConfigsRequest getAllConfigsRequest =
        GetAllConfigsRequest.newBuilder()
            .setResourceName(ANOMALY_EXCLUSION_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(ANOMALY_EXCLUSION_CONFIG_NAMESPACE)
            .build();
    return configServiceBlockingStub.getAllConfigs(getAllConfigsRequest);
  }

  public UpsertConfigResponse upsertExclusionConfigByRuleId(String ruleId, Value config) {
    UpsertConfigRequest upsertConfigRequest =
        UpsertConfigRequest.newBuilder()
            .setResourceName(
                AnomalyExclusionConfigServiceConstants.ANOMALY_EXCLUSION_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(
                AnomalyExclusionConfigServiceConstants.ANOMALY_EXCLUSION_CONFIG_NAMESPACE)
            .setContext(ruleId)
            .setConfig(config)
            .build();
    return configServiceBlockingStub.upsertConfig(upsertConfigRequest);
  }

  public DeleteConfigResponse deleteExclusionConfigByRuleId(String ruleId) {
    DeleteConfigRequest deleteConfigRequest =
        DeleteConfigRequest.newBuilder()
            .setContext(ruleId)
            .setResourceName(
                AnomalyExclusionConfigServiceConstants.ANOMALY_EXCLUSION_CONFIG_RESOURCE_NAME)
            .setResourceNamespace(
                AnomalyExclusionConfigServiceConstants.ANOMALY_EXCLUSION_CONFIG_NAMESPACE)
            .build();
    return configServiceBlockingStub.deleteConfig(deleteConfigRequest);
  }
}
