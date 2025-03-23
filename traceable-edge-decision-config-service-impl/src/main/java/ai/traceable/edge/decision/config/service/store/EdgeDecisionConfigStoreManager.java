package ai.traceable.edge.decision.config.service.store;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionEngineConfigResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigResponse;
import ai.traceable.edge.decision.config.service.v1.UpsertEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.UpsertEdgeDecisionEngineConfigResponse;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EdgeDecisionConfigStoreManager {
  private final EdgeDecisionConfigStore edgeDecisionConfigStore;

  @Inject
  public EdgeDecisionConfigStoreManager(EdgeDecisionConfigStore edgeDecisionConfigStore) {
    this.edgeDecisionConfigStore = edgeDecisionConfigStore;
  }

  public GetEdgeDecisionEngineConfigResponse get(
      RequestContext requestContext, GetEdgeDecisionEngineConfigRequest request) {
    Optional<EdgeDecisionEngineConfig> config =
        edgeDecisionConfigStore.getData(requestContext, request.getId());
    return config
        .map(
            edgeDecisionEngineConfig ->
                GetEdgeDecisionEngineConfigResponse.newBuilder()
                    .setEdgeDecisionEngineConfig(edgeDecisionEngineConfig)
                    .build())
        .orElseGet(GetEdgeDecisionEngineConfigResponse::getDefaultInstance);
  }

  public GetAllEdgeDecisionEngineConfigResponse getAll(
      RequestContext requestContext, GetAllEdgeDecisionEngineConfigRequest request) {
    List<EdgeDecisionEngineConfig> configs =
        edgeDecisionConfigStore.getAllConfigData(requestContext);
    return GetAllEdgeDecisionEngineConfigResponse.newBuilder()
        .addAllEdgeDecisionEngineConfigs(configs)
        .build();
  }

  public UpsertEdgeDecisionEngineConfigResponse upsert(
      RequestContext requestContext, UpsertEdgeDecisionEngineConfigRequest request) {
    if (request.getVersion() <= 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing version on the config")
          .asRuntimeException(requestContext.buildTrailers());
    }
    EdgeDecisionEngineConfig.Builder config = EdgeDecisionEngineConfig.newBuilder();
    if (request.getId().isBlank()) {
      config.setId(getTenantId(requestContext)).build();
    } else {
      config.setId(request.getId());
    }
    config.setName(request.getName());
    config.setVersion(request.getVersion());
    config.addAllCommonVariables(request.getCommonVariablesList());
    config.setDisabled(request.getDisabled());
    config.addAllDisabledRuleCategories(request.getDisabledRuleCategoriesList());
    config.setDefaultBlockResponse(request.getDefaultBlockResponse());
    config.setCustomConfig(request.getCustomConfig());
    ContextualConfigObject<EdgeDecisionEngineConfig> configObject =
        edgeDecisionConfigStore.upsertObject(requestContext, config.build());
    return UpsertEdgeDecisionEngineConfigResponse.newBuilder()
        .setEdgeDecisionEngineConfig(configObject.getData())
        .build();
  }

  private String getTenantId(RequestContext requestContext) {
    return requestContext
        .getTenantId()
        .orElseThrow(() -> new IllegalArgumentException("Tenant Id is missing in request"));
  }
}
