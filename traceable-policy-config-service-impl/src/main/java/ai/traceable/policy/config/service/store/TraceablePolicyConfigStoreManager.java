package ai.traceable.policy.config.service.store;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.policy.config.service.v1.DeleteRequest;
import ai.traceable.policy.config.service.v1.DeleteResponse;
import ai.traceable.policy.config.service.v1.GetAllRequest;
import ai.traceable.policy.config.service.v1.GetAllResponse;
import ai.traceable.policy.config.service.v1.GetRequest;
import ai.traceable.policy.config.service.v1.GetResponse;
import ai.traceable.policy.config.service.v1.TraceablePolicy;
import ai.traceable.policy.config.service.v1.UpsertRequest;
import ai.traceable.policy.config.service.v1.UpsertResponse;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class TraceablePolicyConfigStoreManager {
  private final UuidGenerator uuidGenerator;
  private final TraceablePolicyConfigStore traceablePolicyConfigStore;

  @Inject
  public TraceablePolicyConfigStoreManager(
      UuidGenerator uuidGenerator, TraceablePolicyConfigStore traceablePolicyConfigStore) {
    this.uuidGenerator = uuidGenerator;
    this.traceablePolicyConfigStore = traceablePolicyConfigStore;
  }

  public GetAllResponse getAll(RequestContext requestContext, GetAllRequest request) {
    List<TraceablePolicy> policies = traceablePolicyConfigStore.getAllConfigData(requestContext);
    return GetAllResponse.newBuilder().addAllPolicies(policies).build();
  }

  public GetResponse get(RequestContext requestContext, GetRequest request) {
    Optional<TraceablePolicy> config =
        traceablePolicyConfigStore.getData(requestContext, request.getId());
    return config
        .map(policy -> GetResponse.newBuilder().setPolicy(policy).build())
        .orElseGet(() -> GetResponse.newBuilder().build());
  }

  public UpsertResponse upsert(RequestContext requestContext, UpsertRequest request) {
    var config = request.getPolicy();
    if (config.getId().isEmpty()) {
      config = config.toBuilder().setId(uuidGenerator.generateRandomId()).build();
    }
    ContextualConfigObject<TraceablePolicy> configObject =
        traceablePolicyConfigStore.upsertObject(requestContext, config);
    return UpsertResponse.newBuilder().setPolicy(configObject.getData()).build();
  }

  public DeleteResponse delete(RequestContext requestContext, DeleteRequest request) {
    var deleted = traceablePolicyConfigStore.deleteObject(requestContext, request.getId());
    if (deleted.isPresent() && deleted.get().getDeletedData().isPresent()) {
      return DeleteResponse.newBuilder()
          .setDeletedPolicy(deleted.get().getDeletedData().get())
          .build();
    } else {
      return DeleteResponse.newBuilder().build();
    }
  }
}
