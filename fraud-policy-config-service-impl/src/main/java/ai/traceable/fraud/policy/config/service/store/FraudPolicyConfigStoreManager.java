package ai.traceable.fraud.policy.config.service.store;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicy;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListResponse;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyResponse;
import io.grpc.Status;
import io.grpc.StatusException;
import java.util.List;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ContextualConfigObject;
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
    fetchExistingFraudPolicyOrThrow(request.getFraudPolicyId(), requestContext);
    FraudPolicy updatedFraudPolicy = request.getFraudPolicy();
    ContextualConfigObject<FraudPolicy> configObject =
        fraudPolicyConfigStore.upsertObject(requestContext, updatedFraudPolicy);
    FraudPolicy updated = buildFraudPolicy(configObject);
    return UpdateFraudPolicyResponse.newBuilder().setFraudPolicy(updated).build();
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
        GetFraudPolicyListRequest.newBuilder().addFraudPolicyId(request.getFraudPolicyId()).build();
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

  private FraudPolicy fetchExistingFraudPolicyOrThrow(String id, RequestContext requestContext)
      throws StatusException {
    return fraudPolicyConfigStore
        .getData(requestContext, id)
        .orElseThrow(Status.NOT_FOUND::asException);
  }
}
