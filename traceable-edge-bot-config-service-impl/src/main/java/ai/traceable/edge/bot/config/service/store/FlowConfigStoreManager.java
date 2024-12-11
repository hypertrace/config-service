package ai.traceable.edge.bot.config.service.store;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.edge.bot.config.service.v1.DeleteFlowConfigRequest;
import ai.traceable.edge.bot.config.service.v1.DeleteFlowConfigResponse;
import ai.traceable.edge.bot.config.service.v1.FlowConfig;
import ai.traceable.edge.bot.config.service.v1.GetAllFlowConfigsRequest;
import ai.traceable.edge.bot.config.service.v1.GetAllFlowConfigsResponse;
import ai.traceable.edge.bot.config.service.v1.GetFlowConfigRequest;
import ai.traceable.edge.bot.config.service.v1.GetFlowConfigResponse;
import ai.traceable.edge.bot.config.service.v1.UpsertFlowConfigRequest;
import ai.traceable.edge.bot.config.service.v1.UpsertFlowConfigResponse;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class FlowConfigStoreManager {
  private final UuidGenerator uuidGenerator;
  private final FlowConfigStore flowConfigStore;

  @Inject
  public FlowConfigStoreManager(UuidGenerator uuidGenerator, FlowConfigStore flowConfigStore) {
    this.uuidGenerator = uuidGenerator;
    this.flowConfigStore = flowConfigStore;
  }

  public GetAllFlowConfigsResponse getAll(
      RequestContext requestContext, GetAllFlowConfigsRequest request) {
    List<FlowConfig> configs = flowConfigStore.getAllConfigData(requestContext);
    return GetAllFlowConfigsResponse.newBuilder().addAllConfigs(configs).build();
  }

  public GetFlowConfigResponse get(RequestContext requestContext, GetFlowConfigRequest request) {
    Optional<FlowConfig> config = flowConfigStore.getData(requestContext, request.getId());
    return config
        .map(flowConfig -> GetFlowConfigResponse.newBuilder().setConfig(flowConfig).build())
        .orElseGet(GetFlowConfigResponse::getDefaultInstance);
  }

  public UpsertFlowConfigResponse upsert(
      RequestContext requestContext, UpsertFlowConfigRequest request) {
    FlowConfig config = request.getConfig();
    if (config.getId().isEmpty()) {
      config = config.toBuilder().setId(uuidGenerator.generateRandomId()).build();
    }
    ContextualConfigObject<FlowConfig> configObject =
        flowConfigStore.upsertObject(requestContext, config);
    return UpsertFlowConfigResponse.newBuilder().setConfig(configObject.getData()).build();
  }

  public DeleteFlowConfigResponse delete(
      RequestContext requestContext, DeleteFlowConfigRequest request) {
    var deleted = flowConfigStore.deleteObject(requestContext, request.getId());
    if (deleted.isPresent() && deleted.get().getDeletedData().isPresent()) {
      return DeleteFlowConfigResponse.newBuilder()
          .setDeletedConfig(deleted.get().getDeletedData().get())
          .build();
    } else {
      return DeleteFlowConfigResponse.newBuilder().build();
    }
  }
}
