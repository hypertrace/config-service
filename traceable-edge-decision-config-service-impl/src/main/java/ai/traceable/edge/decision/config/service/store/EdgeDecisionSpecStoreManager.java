package ai.traceable.edge.decision.config.service.store;

import static ai.traceable.config.proto.utils.FieldMaskUtils.applyFieldMask;

import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionSpecRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeDecisionSpecResponse;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeDecisionSpecRequest;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeDecisionSpecResponse;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionSpec;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionSpecsRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeDecisionSpecsResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionSpecRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeDecisionSpecResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionSpecRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeDecisionSpecResponse;
import io.grpc.Status;
import io.grpc.StatusException;
import jakarta.inject.Inject;
import java.util.List;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EdgeDecisionSpecStoreManager {
  private final EdgeDecisionSpecStore edgeDecisionSpecStore;

  @Inject
  public EdgeDecisionSpecStoreManager(EdgeDecisionSpecStore edgeDecisionSpecStore) {
    this.edgeDecisionSpecStore = edgeDecisionSpecStore;
  }

  public CreateEdgeDecisionSpecResponse create(
      RequestContext requestContext, CreateEdgeDecisionSpecRequest request) {
    var edgeDecisionSpec = request.getEdgeDecisionSpec();
    ContextualConfigObject<EdgeDecisionSpec> configObject =
        edgeDecisionSpecStore.upsertObject(requestContext, edgeDecisionSpec);
    return CreateEdgeDecisionSpecResponse.newBuilder()
        .setEdgeDecisionSpec(configObject.getData())
        .build();
  }

  public GetEdgeDecisionSpecResponse get(
      RequestContext requestContext, GetEdgeDecisionSpecRequest request) {
    EdgeDecisionSpec spec;
    try {
      spec = edgeDecisionSpecStore.fetchExisting(request.getId(), requestContext);
    } catch (StatusException e) {
      throw Status.NOT_FOUND
          .withDescription(String.format("EdgeDecisionSpec with id=%s not found", request.getId()))
          .asRuntimeException();
    }
    return GetEdgeDecisionSpecResponse.newBuilder().setEdgeDecisionSpec(spec).build();
  }

  @SneakyThrows
  public UpdateEdgeDecisionSpecResponse update(
      RequestContext requestContext, UpdateEdgeDecisionSpecRequest request) {
    EdgeDecisionSpec existing =
        edgeDecisionSpecStore.fetchExisting(request.getEdgeDecisionSpec().getId(), requestContext);
    if (existing.getVersion() != request.getCurrentVersion()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              String.format(
                  "Received current version=%d, Existing policy version=%d. Read latest and update again",
                  request.getCurrentVersion(), existing.getVersion()))
          .asException();
    }
    // Merge the existing object with the new object, using update masks
    EdgeDecisionSpec updatedResource =
        applyFieldMask(existing, request.getEdgeDecisionSpec(), request.getUpdateMask());
    ;

    ContextualConfigObject<EdgeDecisionSpec> configObject =
        edgeDecisionSpecStore.upsertObject(requestContext, updatedResource);
    return UpdateEdgeDecisionSpecResponse.newBuilder()
        .setEdgeDecisionSpec(configObject.getData())
        .build();
  }

  public GetAllEdgeDecisionSpecsResponse getAll(
      RequestContext requestContext, GetAllEdgeDecisionSpecsRequest request) {
    List<EdgeDecisionSpec> specs = edgeDecisionSpecStore.getAllConfigData(requestContext);
    return GetAllEdgeDecisionSpecsResponse.newBuilder().addAllEdgeDecisionSpecs(specs).build();
  }

  public DeleteEdgeDecisionSpecResponse delete(
      RequestContext requestContext, DeleteEdgeDecisionSpecRequest request) {
    var response = edgeDecisionSpecStore.deleteObject(requestContext, request.getId());
    if (response.isPresent() && response.get().getDeletedData().isPresent()) {
      return DeleteEdgeDecisionSpecResponse.newBuilder()
          .setDeletedEdgeDecisionSpec(response.get().getDeletedData().get())
          .build();
    }
    return DeleteEdgeDecisionSpecResponse.getDefaultInstance();
  }
}
