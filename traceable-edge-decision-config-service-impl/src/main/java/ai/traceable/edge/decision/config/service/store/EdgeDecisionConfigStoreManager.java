package ai.traceable.edge.decision.config.service.store;

import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionEngineConfigResponse;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionEngineConfigResponse;
import io.grpc.Status;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
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

  public CreateEdgeDecisionEngineConfigResponse create(
      RequestContext requestContext, CreateEdgeDecisionEngineConfigRequest request) {
    var config = request.getEdgeDecisionEngineConfig();
    if (config.getId().isBlank()) {
      config = config.toBuilder().setId(getTenantId(requestContext)).build();
    }
    if (config.getVersion() <= 0) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Missing version on the config")
          .asRuntimeException(requestContext.buildTrailers());
    }
    ContextualConfigObject<EdgeDecisionEngineConfig> configObject =
        edgeDecisionConfigStore.upsertObject(requestContext, config);
    return CreateEdgeDecisionEngineConfigResponse.newBuilder()
        .setEdgeDecisionEngineConfig(configObject.getData())
        .build();
  }

  @SneakyThrows
  public UpdateEdgeDecisionEngineConfigResponse update(
      RequestContext requestContext, UpdateEdgeDecisionEngineConfigRequest request) {
    EdgeDecisionEngineConfig config = request.getEdgeDecisionEngineConfig();
    EdgeDecisionEngineConfig existing =
        edgeDecisionConfigStore.fetchExisting(config.getId(), requestContext);
    if (existing.getVersion() != request.getCurrentVersion()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              String.format(
                  "Received current version=%d, Existing policy version=%d. Read latest and update again",
                  request.getCurrentVersion(), existing.getVersion()))
          .asException();
    }
    ContextualConfigObject<EdgeDecisionEngineConfig> configObject =
        edgeDecisionConfigStore.upsertObject(requestContext, config);
    return UpdateEdgeDecisionEngineConfigResponse.newBuilder()
        .setEdgeDecisionEngineConfig(configObject.getData())
        .build();
  }

  public GetEdgeDecisionEngineConfigResponse get(RequestContext requestContext, String id) {
    EdgeDecisionEngineConfig config =
        edgeDecisionConfigStore
            .getData(requestContext, id)
            .orElse(EdgeDecisionEngineConfig.getDefaultInstance());
    return GetEdgeDecisionEngineConfigResponse.newBuilder()
        .setEdgeDecisionEngineConfig(config)
        .build();
  }

  private String getTenantId(RequestContext requestContext) {
    return requestContext
        .getTenantId()
        .orElseThrow(() -> new IllegalArgumentException("Tenant Id is missing in request"));
  }
}
