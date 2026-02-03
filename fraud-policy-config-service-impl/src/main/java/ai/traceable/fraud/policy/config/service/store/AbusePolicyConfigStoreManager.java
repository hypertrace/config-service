package ai.traceable.fraud.policy.config.service.store;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.policy.config.service.v1.AbusePolicy;
import ai.traceable.fraud.policy.config.service.v1.CreateAbusePolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.CreateAbusePolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.DeleteAbusePolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteAbusePolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.GetAbusePoliciesRequest;
import ai.traceable.fraud.policy.config.service.v1.GetAbusePoliciesResponse;
import ai.traceable.fraud.policy.config.service.v1.GetAbusePolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.GetAbusePolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.UpdateAbusePolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateAbusePolicyResponse;
import io.grpc.Status;
import io.grpc.StatusException;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.DeletedContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class AbusePolicyConfigStoreManager {

  private final AbusePolicyConfigStore abusePolicyConfigStore;
  private final UuidGenerator uuidGenerator;

  @Inject
  public AbusePolicyConfigStoreManager(
      AbusePolicyConfigStore abusePolicyConfigStore, UuidGenerator uuidGenerator) {
    this.abusePolicyConfigStore = abusePolicyConfigStore;
    this.uuidGenerator = uuidGenerator;
  }

  public CreateAbusePolicyResponse createAbusePolicy(
      RequestContext requestContext, CreateAbusePolicyRequest request) {
    // Generate ID and build policy
    String policyId = uuidGenerator.generateRandomId();
    AbusePolicy abusePolicy =
        AbusePolicy.newBuilder().setId(policyId).setData(request.getData()).build();

    ContextualConfigObject<AbusePolicy> configObject =
        abusePolicyConfigStore.upsertObject(requestContext, abusePolicy);
    AbusePolicy created = buildAbusePolicy(configObject);
    return CreateAbusePolicyResponse.newBuilder().setPolicy(created).build();
  }

  public UpdateAbusePolicyResponse updateAbusePolicy(
      RequestContext requestContext, UpdateAbusePolicyRequest request) throws StatusException {
    // Check existence of the policy before updating
    AbusePolicy existing = fetchExistingAbusePolicyOrThrow(request.getPolicyId(), requestContext);

    // Do not allow update if the version provided is not the same as the one in the store
    if (existing.getData().getVersion() != request.getData().getVersion()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              String.format(
                  "Received version=%d, Existing policy version=%d. Read latest and update again",
                  request.getData().getVersion(), existing.getData().getVersion()))
          .asException();
    }

    // Build updated policy with incremented version
    AbusePolicy updatedPolicy =
        AbusePolicy.newBuilder()
            .setId(request.getPolicyId())
            .setData(
                request.getData().toBuilder()
                    .setVersion(existing.getData().getVersion() + 1)
                    .build())
            .build();

    ContextualConfigObject<AbusePolicy> configObject =
        abusePolicyConfigStore.upsertObject(requestContext, updatedPolicy);
    AbusePolicy updated = buildAbusePolicy(configObject);
    return UpdateAbusePolicyResponse.newBuilder().setPolicy(updated).build();
  }

  public DeleteAbusePolicyResponse deleteAbusePolicyList(
      RequestContext requestContext, DeleteAbusePolicyRequest request) throws StatusException {
    List<String> ids = request.getPolicyIdsList();
    if (ids.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("No policy IDs provided for deletion.")
          .asException();
    }

    List<DeletedContextualConfigObject<AbusePolicy>> configObjects =
        abusePolicyConfigStore.deleteObjects(requestContext, ids);
    return DeleteAbusePolicyResponse.newBuilder()
        .addAllPolicies(buildAbusePolicies(configObjects))
        .build();
  }

  public GetAbusePoliciesResponse fetchAbusePolicies(
      RequestContext requestContext, GetAbusePoliciesRequest request) {
    List<AbusePolicy> abusePolicies =
        abusePolicyConfigStore.getAllConfigData(requestContext, request);
    return GetAbusePoliciesResponse.newBuilder().addAllPolicies(abusePolicies).build();
  }

  public GetAbusePolicyResponse fetchAbusePolicy(
      RequestContext requestContext, GetAbusePolicyRequest request) throws StatusException {
    AbusePolicy policy = fetchExistingAbusePolicyOrThrow(request.getPolicyId(), requestContext);
    return GetAbusePolicyResponse.newBuilder().setPolicy(policy).build();
  }

  private AbusePolicy buildAbusePolicy(ContextualConfigObject<AbusePolicy> configObject) {
    return AbusePolicy.newBuilder(configObject.getData()).build();
  }

  private List<AbusePolicy> buildAbusePolicies(
      List<DeletedContextualConfigObject<AbusePolicy>> configObjects) {
    return configObjects.stream()
        .map(DeletedContextualConfigObject::getDeletedData)
        .filter(Optional::isPresent)
        .map(data -> AbusePolicy.newBuilder(data.get()).build())
        .collect(Collectors.toList());
  }

  private AbusePolicy fetchExistingAbusePolicyOrThrow(String id, RequestContext requestContext)
      throws StatusException {
    return abusePolicyConfigStore
        .getData(requestContext, id)
        .orElseThrow(
            () ->
                Status.NOT_FOUND
                    .withDescription("No abuse policy found with id=" + id)
                    .asException());
  }
}
