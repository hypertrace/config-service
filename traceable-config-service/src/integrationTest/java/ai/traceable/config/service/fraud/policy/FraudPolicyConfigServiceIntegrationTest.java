package ai.traceable.config.service.fraud.policy;

import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.config.service.fraud.ResourceUtils;
import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicy;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyConfigServiceGrpc;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyList;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyRequest;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FraudPolicyConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static FraudPolicyConfigServiceGrpc.FraudPolicyConfigServiceBlockingStub serviceStub;

  @BeforeAll
  static void init() {
    serviceStub =
        FraudPolicyConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  public void testCRUD() throws IOException {
    FraudPolicyList fraudPolicyList =
        ResourceUtils.readProto("fraud/policy/policies.json", FraudPolicyList.newBuilder()).build();
    FraudPolicy fraudPolicy = fraudPolicyList.getPolicies(0).toBuilder().clearId().build();
    CreateFraudPolicyRequest createFraudPolicyRequest =
        CreateFraudPolicyRequest.newBuilder().setFraudPolicy(fraudPolicy).build();
    var createFraudPolicyResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () -> {
                  return serviceStub.createFraudPolicy(createFraudPolicyRequest);
                });
    Assertions.assertTrue(createFraudPolicyResponse.hasFraudPolicy());
    FraudPolicy saved = createFraudPolicyResponse.getFraudPolicy();
    Assertions.assertFalse(StringUtils.isBlank(saved.getId()));
    // check everything is same except id
    Assertions.assertEquals(
        createFraudPolicyRequest.getFraudPolicy(), fraudPolicy.toBuilder().clearId().build());

    FraudPolicy updatedFraudPolicy =
        fraudPolicy.toBuilder().setId(saved.getId()).setName("updated_fraud_policy").build();
    UpdateFraudPolicyRequest updateFraudPolicyRequest =
        UpdateFraudPolicyRequest.newBuilder()
            .setFraudPolicyId(updatedFraudPolicy.getId())
            .setFraudPolicy(updatedFraudPolicy)
            .setUpdateMask(FieldMask.newBuilder().addPaths("name").addPaths("fraud_policy"))
            .build();
    var updateFraudPolicyResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(() -> serviceStub.updateFraudPolicy(updateFraudPolicyRequest));
    Assertions.assertEquals(
        updatedFraudPolicy.getName(), updateFraudPolicyResponse.getFraudPolicy().getName());

    var getFraudPolicyListResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    serviceStub.getFraudPolicyList(GetFraudPolicyListRequest.getDefaultInstance()));
    Assertions.assertEquals(1, getFraudPolicyListResponse.getFraudPolicyListCount());
    Assertions.assertEquals(updatedFraudPolicy, getFraudPolicyListResponse.getFraudPolicyList(0));

    getFraudPolicyListResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    serviceStub.getFraudPolicyList(
                        GetFraudPolicyListRequest.newBuilder()
                            .addFraudPolicyId(updatedFraudPolicy.getId())
                            .build()));
    Assertions.assertEquals(1, getFraudPolicyListResponse.getFraudPolicyListCount());
    Assertions.assertEquals(updatedFraudPolicy, getFraudPolicyListResponse.getFraudPolicyList(0));

    getFraudPolicyListResponse =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    serviceStub.getFraudPolicyList(
                        GetFraudPolicyListRequest.newBuilder()
                            .addFraudPolicyId("non_existent")
                            .build()));
    Assertions.assertEquals(0, getFraudPolicyListResponse.getFraudPolicyListCount());
  }

  @Test
  public void testUpdateFailsOnNonExistentConfig() {
    UpdateFraudPolicyRequest updateFraudPolicyRequest =
        UpdateFraudPolicyRequest.newBuilder()
            .setFraudPolicy(FraudPolicy.getDefaultInstance())
            .build();
    try {
      RequestContext.forTenantId(TENANT_ID)
          .call(() -> serviceStub.updateFraudPolicy(updateFraudPolicyRequest));
      Assertions.fail("Expected update to fail");
    } catch (StatusRuntimeException e) {
      Assertions.assertEquals(e.getStatus(), Status.NOT_FOUND);
    }
  }
}
