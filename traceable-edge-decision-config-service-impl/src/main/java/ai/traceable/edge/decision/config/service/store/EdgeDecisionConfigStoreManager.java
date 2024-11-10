package ai.traceable.edge.decision.config.service.store;

import static ai.traceable.edge.decision.config.service.store.EdgeDecisionConfigStore.getConfigId;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigResponse;
import ai.traceable.edge.decision.config.service.v1.UpsertEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.UpsertEdgeDecisionEngineConfigResponse;
import io.grpc.Status;
import java.util.Optional;
import javax.inject.Inject;
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
    var configId = getConfigId(getTenantId(requestContext), request.getVersion());
    Optional<EdgeDecisionEngineConfig> config =
        edgeDecisionConfigStore.getData(requestContext, configId);
    return config
        .map(
            edgeDecisionEngineConfig ->
                GetEdgeDecisionEngineConfigResponse.newBuilder()
                    .setEdgeDecisionEngineConfig(edgeDecisionEngineConfig)
                    .build())
        .orElseGet(() -> GetEdgeDecisionEngineConfigResponse.newBuilder().build());
  }

  public UpsertEdgeDecisionEngineConfigResponse upsert(
      RequestContext requestContext, UpsertEdgeDecisionEngineConfigRequest request) {
    var config = request.getEdgeDecisionEngineConfig();
    if (config.getVersion().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing version on the config")
          .asRuntimeException(requestContext.buildTrailers());
    }
    config = config.toBuilder().setId(getTenantId(requestContext)).build();
    ContextualConfigObject<EdgeDecisionEngineConfig> configObject =
        edgeDecisionConfigStore.upsertObject(requestContext, config);
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
