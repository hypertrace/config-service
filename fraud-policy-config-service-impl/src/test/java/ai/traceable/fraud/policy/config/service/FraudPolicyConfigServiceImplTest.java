package ai.traceable.fraud.policy.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.policy.config.service.store.FraudPolicyConfigStore;
import ai.traceable.fraud.policy.config.service.store.FraudPolicyConfigStoreManager;
import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicy;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyConfigServiceGrpc;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListResponse;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.validation.FraudPolicyConfigRequestValidator;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FraudPolicyConfigServiceImplTest {

  private static final String UUID_1 = "uuid-1";

  private FraudPolicyConfigServiceGrpc.FraudPolicyConfigServiceBlockingStub
      fraudPolicyConfigServiceBlockingStub;
  private FraudPolicyConfigStoreManager storeManager;
  private MockGenericConfigService mockGenericConfigService;
  @Mock private ConfigChangeEventGenerator eventGenerator;
  @Mock private UuidGenerator uuidGenerator;

  @BeforeEach
  void beforeEach() {
    this.mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    this.storeManager =
        new FraudPolicyConfigStoreManager(
            new FraudPolicyConfigStore(genericStub, eventGenerator), uuidGenerator);
    this.mockGenericConfigService
        .addService(
            new FraudPolicyConfigServiceImpl(storeManager, new FraudPolicyConfigRequestValidator()))
        .start();

    this.fraudPolicyConfigServiceBlockingStub =
        FraudPolicyConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    when(uuidGenerator.generateRandomId()).thenReturn(UUID_1);
  }

  @AfterEach
  void afterEach() {
    this.mockGenericConfigService.shutdown();
  }

  @Test
  void testDerivationConfigCRUD() {
    RequestContext requestContext = buildRequestContext();
    FraudPolicy createdFraudPolicy =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub
                    .createFraudPolicy(
                        CreateFraudPolicyRequest.newBuilder()
                            .setFraudPolicy(FraudPolicy.newBuilder().setName("created"))
                            .build())
                    .getFraudPolicy());

    assertEquals(UUID_1, createdFraudPolicy.getId());

    FraudPolicy updatedFraudPolicy =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub
                    .updateFraudPolicy(
                        UpdateFraudPolicyRequest.newBuilder()
                            .setFraudPolicyId(UUID_1)
                            .setFraudPolicy(
                                FraudPolicy.newBuilder().setId(UUID_1).setName("updated"))
                            .build())
                    .getFraudPolicy());

    assertEquals("updated", updatedFraudPolicy.getName());

    GetFraudPolicyListResponse fraudPolicyListResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.getFraudPolicyList(
                    GetFraudPolicyListRequest.getDefaultInstance()));
    assertEquals(1, fraudPolicyListResponse.getFraudPolicyListCount());
    assertEquals(UUID_1, fraudPolicyListResponse.getFraudPolicyList(0).getId());

    GetFraudPolicyResponse fraudPolicyResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.getFraudPolicy(
                    GetFraudPolicyRequest.newBuilder().setFraudPolicyId(UUID_1).build()));
    assertEquals(UUID_1, fraudPolicyResponse.getFraudPolicy().getId());

    storeManager.deleteDerivedConfig(requestContext, UUID_1);

    fraudPolicyListResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.getFraudPolicyList(
                    GetFraudPolicyListRequest.getDefaultInstance()));
    assertEquals(0, fraudPolicyListResponse.getFraudPolicyListCount());
  }

  private static RequestContext buildRequestContext() {
    return RequestContext.forTenantId("t1");
  }
}
