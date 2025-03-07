package ai.traceable.edge.decision.config.service.store;

import static ai.traceable.config.proto.utils.FieldMaskUtils.applyFieldMask;

import ai.traceable.edge.decision.config.service.v1.CreateEdgeCustomResponseRequest;
import ai.traceable.edge.decision.config.service.v1.CreateEdgeCustomResponseResponse;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeCustomResponseRequest;
import ai.traceable.edge.decision.config.service.v1.DeleteEdgeCustomResponseResponse;
import ai.traceable.edge.decision.config.service.v1.EdgeCustomResponse;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeCustomResponsesRequest;
import ai.traceable.edge.decision.config.service.v1.GetAllEdgeCustomResponsesResponse;
import ai.traceable.edge.decision.config.service.v1.GetEdgeCustomResponseRequest;
import ai.traceable.edge.decision.config.service.v1.GetEdgeCustomResponseResponse;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeCustomResponseRequest;
import ai.traceable.edge.decision.config.service.v1.UpdateEdgeCustomResponseResponse;
import io.grpc.Status;
import io.grpc.StatusException;
import jakarta.inject.Inject;
import java.util.List;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EdgeCustomResponseStoreManager {
  private final EdgeCustomResponseStore edgeCustomResponseStore;

  @Inject
  public EdgeCustomResponseStoreManager(EdgeCustomResponseStore edgeCustomResponseStore) {
    this.edgeCustomResponseStore = edgeCustomResponseStore;
  }

  public CreateEdgeCustomResponseResponse create(
      RequestContext requestContext, CreateEdgeCustomResponseRequest request) {
    var edgeCustomResponse = request.getEdgeCustomResponse();
    ContextualConfigObject<EdgeCustomResponse> configObject =
        edgeCustomResponseStore.upsertObject(requestContext, edgeCustomResponse);
    return CreateEdgeCustomResponseResponse.newBuilder()
        .setEdgeCustomResponse(configObject.getData())
        .build();
  }

  public GetEdgeCustomResponseResponse get(
      RequestContext requestContext, GetEdgeCustomResponseRequest request) {
    EdgeCustomResponse customResponse;
    try {
      customResponse = edgeCustomResponseStore.fetchExisting(request.getId(), requestContext);
    } catch (StatusException e) {
      throw Status.NOT_FOUND
          .withDescription(
              String.format("EdgeCustomResponse with id=%s not found", request.getId()))
          .asRuntimeException();
    }
    return GetEdgeCustomResponseResponse.newBuilder().setEdgeCustomResponse(customResponse).build();
  }

  @SneakyThrows
  public UpdateEdgeCustomResponseResponse update(
      RequestContext requestContext, UpdateEdgeCustomResponseRequest request) {
    EdgeCustomResponse existing =
        edgeCustomResponseStore.fetchExisting(
            request.getEdgeCustomResponse().getId(), requestContext);
    if (existing.getVersion() != request.getCurrentVersion()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              String.format(
                  "Received current version=%d, Existing policy version=%d. Read latest and update again",
                  request.getCurrentVersion(), existing.getVersion()))
          .asException();
    }
    // Merge the existing object with the new object, using update masks
    EdgeCustomResponse updatedResource =
        applyFieldMask(existing, request.getEdgeCustomResponse(), request.getUpdateMask());
    ;

    ContextualConfigObject<EdgeCustomResponse> configObject =
        edgeCustomResponseStore.upsertObject(requestContext, updatedResource);
    return UpdateEdgeCustomResponseResponse.newBuilder()
        .setEdgeCustomResponse(configObject.getData())
        .build();
  }

  public GetAllEdgeCustomResponsesResponse getAll(
      RequestContext requestContext, GetAllEdgeCustomResponsesRequest request) {
    List<EdgeCustomResponse> customResponses =
        edgeCustomResponseStore.getAllConfigData(requestContext);
    return GetAllEdgeCustomResponsesResponse.newBuilder()
        .addAllEdgeCustomResponses(customResponses)
        .build();
  }

  public DeleteEdgeCustomResponseResponse delete(
      RequestContext requestContext, DeleteEdgeCustomResponseRequest request) {
    var response = edgeCustomResponseStore.deleteObject(requestContext, request.getId());
    if (response.isPresent() && response.get().getDeletedData().isPresent()) {
      return DeleteEdgeCustomResponseResponse.newBuilder()
          .setDeletedEdgeCustomResponse(response.get().getDeletedData().get())
          .build();
    }
    return DeleteEdgeCustomResponseResponse.getDefaultInstance();
  }
}
