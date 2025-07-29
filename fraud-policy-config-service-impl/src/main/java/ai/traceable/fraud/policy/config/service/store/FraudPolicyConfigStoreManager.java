package ai.traceable.fraud.policy.config.service.store;

import static ai.traceable.config.proto.utils.FieldMaskUtils.applyFieldMask;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.DeleteFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicy;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListResponse;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.UpsertFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpsertFraudPolicyResponse;
import io.grpc.Status;
import io.grpc.StatusException;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class FraudPolicyConfigStoreManager {

  private final FraudPolicyConfigStore fraudPolicyConfigStore;
  private final UuidGenerator uuidGenerator;

  @Inject
  public FraudPolicyConfigStoreManager(
      FraudPolicyConfigStore fraudPolicyConfigStore, UuidGenerator uuidGenerator) {
    this.fraudPolicyConfigStore = fraudPolicyConfigStore;
    this.uuidGenerator = uuidGenerator;
  }

  public CreateFraudPolicyResponse createFraudPolicy(
      RequestContext requestContext, CreateFraudPolicyRequest request) {
    FraudPolicy.Builder fraudPolicyBuilder = request.getFraudPolicy().toBuilder();
    if (fraudPolicyBuilder.getId().isEmpty()) {
      fraudPolicyBuilder.setId(uuidGenerator.generateRandomId());
    }
    ContextualConfigObject<FraudPolicy> configObject =
        fraudPolicyConfigStore.upsertObject(requestContext, fraudPolicyBuilder.build());
    FraudPolicy upserted = buildFraudPolicy(configObject);
    return CreateFraudPolicyResponse.newBuilder().setFraudPolicy(upserted).build();
  }

  public UpdateFraudPolicyResponse updateFraudPolicy(
      RequestContext requestContext, UpdateFraudPolicyRequest request) throws StatusException {
    // check existence of the policy before updating.
    FraudPolicy existing =
        fetchExistingFraudPolicyOrThrow(request.getFraudPolicyId(), requestContext);
    // do not allow update if the version provided is not the same as the one in the store.
    // caller must get latest and update if this exception is thrown
    if (existing.getVersion() != request.getCurrentVersion()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              String.format(
                  "Received current version=%d, Existing policy version=%d. Read latest and update again",
                  request.getCurrentVersion(), existing.getVersion()))
          .asException();
    }
    FraudPolicy updatedFraudPolicy =
        applyFieldMask(existing, request.getFraudPolicy(), request.getUpdateMask());
    ContextualConfigObject<FraudPolicy> configObject =
        fraudPolicyConfigStore.upsertObject(requestContext, updatedFraudPolicy);
    FraudPolicy updated = buildFraudPolicy(configObject);
    return UpdateFraudPolicyResponse.newBuilder().setFraudPolicy(updated).build();
  }

  public UpsertFraudPolicyResponse upsertFraudPolicy(
      RequestContext requestContext, UpsertFraudPolicyRequest request) throws StatusException {
    FraudPolicy fraudPolicy = request.getFraudPolicy();
    ContextualConfigObject<FraudPolicy> configObject =
        fraudPolicyConfigStore.upsertObject(requestContext, fraudPolicy);
    FraudPolicy upserted = buildFraudPolicy(configObject);
    return UpsertFraudPolicyResponse.newBuilder().setFraudPolicy(upserted).build();
  }

  public DeleteFraudPolicyResponse deleteFraudPolicyList(
      RequestContext requestContext, DeleteFraudPolicyRequest request) throws StatusException {
    List<String> ids = request.getFraudPolicyIdListList();
    if (ids.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("No policy IDs provided for deletion.")
          .asException();
    }
    List<DeletedContextualConfigObject<FraudPolicy>> configObject =
        fraudPolicyConfigStore.deleteObjects(requestContext, ids);
    return DeleteFraudPolicyResponse.newBuilder()
        .addAllFraudPolicyList(buildFraudPolicies(configObject))
        .build();
  }

  public GetFraudPolicyListResponse fetchFraudPolicyList(
      RequestContext requestContext, GetFraudPolicyListRequest request) {
    List<FraudPolicy> fraudPolicies =
        fraudPolicyConfigStore.getAllConfigData(requestContext, request);
    return GetFraudPolicyListResponse.newBuilder().addAllFraudPolicyList(fraudPolicies).build();
  }

  public GetFraudPolicyResponse fetchFraudPolicy(
      RequestContext requestContext, GetFraudPolicyRequest request) throws StatusException {
    GetFraudPolicyListRequest getFraudPolicyListRequest =
        GetFraudPolicyListRequest.newBuilder()
            .addFraudPolicyId(request.getFraudPolicyId())
            .setIncludeDisabled(request.getIncludeDisabled())
            .build();
    GetFraudPolicyListResponse fraudPolicyListResponse =
        fetchFraudPolicyList(requestContext, getFraudPolicyListRequest);
    if (fraudPolicyListResponse.getFraudPolicyListCount() == 0) {
      throw Status.NOT_FOUND
          .withDescription("No fraud pollicy of id=" + request.getFraudPolicyId())
          .asException();
    }
    if (fraudPolicyListResponse.getFraudPolicyListCount() > 1) {
      throw Status.INTERNAL
          .withDescription(
              String.format(
                  "%d fraud policies found with id=%s",
                  fraudPolicyListResponse.getFraudPolicyListCount(), request.getFraudPolicyId()))
          .asException();
    }
    return GetFraudPolicyResponse.newBuilder()
        .setFraudPolicy(fraudPolicyListResponse.getFraudPolicyList(0))
        .build();
  }

  public void deleteDerivedConfig(RequestContext requestContext, String id) {
    fraudPolicyConfigStore.deleteObject(requestContext, id);
  }

  private FraudPolicy buildFraudPolicy(ContextualConfigObject<FraudPolicy> configObject) {
    return FraudPolicy.newBuilder(configObject.getData()).build();
  }

  private List<FraudPolicy> buildFraudPolicies(
      List<DeletedContextualConfigObject<FraudPolicy>> configObjects) {
    return configObjects.stream()
        .map(DeletedContextualConfigObject::getDeletedData)
        .filter(Optional::isPresent)
        .map(data -> FraudPolicy.newBuilder(data.get()).build())
        .collect(Collectors.toList());
  }

  private FraudPolicy fetchExistingFraudPolicyOrThrow(String id, RequestContext requestContext)
      throws StatusException {
    return fraudPolicyConfigStore
        .getData(requestContext, id)
        .orElseThrow(Status.NOT_FOUND::asException);
  }
}
