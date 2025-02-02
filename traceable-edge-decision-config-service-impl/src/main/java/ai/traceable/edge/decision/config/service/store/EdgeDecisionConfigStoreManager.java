package ai.traceable.edge.decision.config.service.store;

import static ai.traceable.config.proto.utils.FieldMaskUtils.applyFieldMask;

import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionEngineConfigResponse;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionEngineConfigResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionEngineConfigResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionEngineConfigRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionEngineConfigResponse;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
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

  public GetAllEdgeDecisionEngineConfigResponse getAll(
      RequestContext requestContext, GetAllEdgeDecisionEngineConfigRequest request) {
    List<EdgeDecisionEngineConfig> configs =
        edgeDecisionConfigStore.getAllConfigData(requestContext);
    return GetAllEdgeDecisionEngineConfigResponse.newBuilder()
        .addAllEdgeDecisionEngineConfigs(configs)
        .build();
  }

  public CreateEdgeDecisionEngineConfigResponse create(
      RequestContext requestContext, CreateEdgeDecisionEngineConfigRequest request) {
    EdgeDecisionEngineConfig config = request.getEdgeDecisionEngineConfig();
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
    EdgeDecisionEngineConfig existing =
        edgeDecisionConfigStore.fetchExisting(
            request.getEdgeDecisionEngineConfig().getId(), requestContext);
    if (existing.getVersion() != request.getCurrentVersion()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              String.format(
                  "Received current version=%d, Existing policy version=%d. Read latest and update again",
                  request.getCurrentVersion(), existing.getVersion()))
          .asException();
    }
    // Merge the existing object with the new object, using update masks
    EdgeDecisionEngineConfig updatedResource =
        applyFieldMask(existing, request.getEdgeDecisionEngineConfig(), request.getUpdateMask());

    ContextualConfigObject<EdgeDecisionEngineConfig> configObject =
        edgeDecisionConfigStore.upsertObject(requestContext, updatedResource);
    return UpdateEdgeDecisionEngineConfigResponse.newBuilder()
        .setEdgeDecisionEngineConfig(configObject.getData())
        .build();
  }

  private String getTenantId(RequestContext requestContext) {
    return requestContext
        .getTenantId()
        .orElseThrow(() -> new IllegalArgumentException("Tenant Id is missing in request"));
  }
}
