package ai.traceable.fraud.policy.config.service.store;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.policy.config.service.v1.ApiAccessAnomalyConfig;
import ai.traceable.fraud.policy.config.service.v1.CreateApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.CreateApiAccessAnomalyConfigResponse;
import ai.traceable.fraud.policy.config.service.v1.DeleteApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteApiAccessAnomalyConfigResponse;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigResponse;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigsRequest;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigsResponse;
import ai.traceable.fraud.policy.config.service.v1.UpdateApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateApiAccessAnomalyConfigResponse;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class ApiAccessAnomalyConfigStoreManager {
  private UuidGenerator uuidGenerator;

  private ApiAccessAnomalyConfigStore apiAccessAnomalyConfigStore;

  @Inject
  public ApiAccessAnomalyConfigStoreManager(
      UuidGenerator uuidGenerator, ApiAccessAnomalyConfigStore apiAccessAnomalyConfigStore) {
    this.uuidGenerator = uuidGenerator;
    this.apiAccessAnomalyConfigStore = apiAccessAnomalyConfigStore;
  }

  public GetApiAccessAnomalyConfigsResponse getApiAccessAnomalyConfigs(
      RequestContext requestContext, GetApiAccessAnomalyConfigsRequest request) {
    List<ApiAccessAnomalyConfig> configs =
        apiAccessAnomalyConfigStore.getAllConfigData(requestContext);
    return GetApiAccessAnomalyConfigsResponse.newBuilder().addAllConfigs(configs).build();
  }

  public GetApiAccessAnomalyConfigResponse getApiAccessAnomalyConfig(
      RequestContext requestContext, GetApiAccessAnomalyConfigRequest request) {
    Optional<ApiAccessAnomalyConfig> config =
        apiAccessAnomalyConfigStore.getData(requestContext, request.getId());
    return config
        .map(
            apiAccessAnomalyConfig ->
                GetApiAccessAnomalyConfigResponse.newBuilder()
                    .setConfig(apiAccessAnomalyConfig)
                    .build())
        .orElseGet(() -> GetApiAccessAnomalyConfigResponse.newBuilder().build());
  }

  public CreateApiAccessAnomalyConfigResponse createApiAccessAnomalyConfig(
      RequestContext requestContext, CreateApiAccessAnomalyConfigRequest request) {
    ApiAccessAnomalyConfig.Builder configBuilder = ApiAccessAnomalyConfig.newBuilder();
    configBuilder.setId(uuidGenerator.generateRandomId());
    configBuilder.addAllGroupedConfigs(request.getGroupedConfigsList());
    ContextualConfigObject<ApiAccessAnomalyConfig> configObject =
        apiAccessAnomalyConfigStore.upsertObject(requestContext, configBuilder.build());
    return CreateApiAccessAnomalyConfigResponse.newBuilder()
        .setConfig(configObject.getData())
        .build();
  }

  public UpdateApiAccessAnomalyConfigResponse updateApiAccessAnomalyConfig(
      RequestContext requestContext, UpdateApiAccessAnomalyConfigRequest request) {
    ContextualConfigObject<ApiAccessAnomalyConfig> configObject =
        apiAccessAnomalyConfigStore.upsertObject(requestContext, request.getConfig());
    return UpdateApiAccessAnomalyConfigResponse.newBuilder()
        .setConfig(configObject.getData())
        .build();
  }

  public DeleteApiAccessAnomalyConfigResponse deleteApiAccessAnomalyConfig(
      RequestContext requestContext, DeleteApiAccessAnomalyConfigRequest request) {
    apiAccessAnomalyConfigStore.deleteObject(requestContext, request.getId());
    return DeleteApiAccessAnomalyConfigResponse.newBuilder().build();
  }
}
