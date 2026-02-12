package ai.traceable.fraud.policy.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import ai.traceable.config.proto.utils.FieldMaskUtils;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.fraud.policy.config.service.store.AbusePolicyConfigStore;
import ai.traceable.fraud.policy.config.service.store.AbusePolicyConfigStoreManager;
import ai.traceable.fraud.policy.config.service.store.ApiAccessAnomalyConfigStore;
import ai.traceable.fraud.policy.config.service.store.ApiAccessAnomalyConfigStoreManager;
import ai.traceable.fraud.policy.config.service.store.FraudPolicyConfigStore;
import ai.traceable.fraud.policy.config.service.store.FraudPolicyConfigStoreManager;
import ai.traceable.fraud.policy.config.service.v1.APISpec;
import ai.traceable.fraud.policy.config.service.v1.ApiAccessAnomalyConfig;
import ai.traceable.fraud.policy.config.service.v1.ApiCollection;
import ai.traceable.fraud.policy.config.service.v1.ApiReference;
import ai.traceable.fraud.policy.config.service.v1.CorrelationKey;
import ai.traceable.fraud.policy.config.service.v1.CreateApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.DeleteFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicy;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyConfigServiceGrpc;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.GetApiAccessAnomalyConfigsRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListResponse;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyResponse;
import ai.traceable.fraud.policy.config.service.v1.GroupedConfig;
import ai.traceable.fraud.policy.config.service.v1.TimeObj;
import ai.traceable.fraud.policy.config.service.v1.TimeUnit;
import ai.traceable.fraud.policy.config.service.v1.TimeWindow;
import ai.traceable.fraud.policy.config.service.v1.UpdateAbusePolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.validation.AbusePolicyConfigRequestValidator;
import ai.traceable.fraud.policy.config.service.validation.ApiAccessAnomalyConfigServiceRequestValidator;
import ai.traceable.fraud.policy.config.service.validation.FraudPolicyConfigRequestValidator;
import com.google.protobuf.FieldMask;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
class FraudPolicyConfigServiceImplTest {

  private static final String UUID_1 = "uuid-1";

  private FraudPolicyConfigServiceGrpc.FraudPolicyConfigServiceBlockingStub
      fraudPolicyConfigServiceBlockingStub;
  private FraudPolicyConfigStoreManager storeManager;
  private ApiAccessAnomalyConfigStoreManager apiAccessAnomalyConfigStoreManager;
  private AbusePolicyConfigStoreManager abusePolicyConfigStoreManager;

  private MockGenericConfigService mockGenericConfigService;
  @Mock private ConfigChangeEventGenerator eventGenerator;
  @Mock private UuidGenerator uuidGenerator;

  @BeforeEach
  void setUp(TestInfo testInfo) {
    if (testInfo.getTags().contains("fraudPolicy")) {
      this.mockGenericConfigService =
          new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDeleteAll();
    } else if (testInfo.getTags().contains("apiAccessAnomaly")) {
      this.mockGenericConfigService =
          new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    } else if (testInfo.getTags().contains("abusePolicy")) {
      this.mockGenericConfigService =
          new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDeleteAll();
    }
  }

  @BeforeEach
  void beforeEach() {
    ConfigServiceGrpc.ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    this.storeManager =
        new FraudPolicyConfigStoreManager(
            new FraudPolicyConfigStore(genericStub, eventGenerator), uuidGenerator);
    this.apiAccessAnomalyConfigStoreManager =
        new ApiAccessAnomalyConfigStoreManager(
            uuidGenerator, new ApiAccessAnomalyConfigStore(genericStub, eventGenerator));
    this.abusePolicyConfigStoreManager =
        new AbusePolicyConfigStoreManager(
            new AbusePolicyConfigStore(genericStub, eventGenerator), uuidGenerator);
    this.mockGenericConfigService
        .addService(
            new FraudPolicyConfigServiceImpl(
                storeManager,
                new FraudPolicyConfigRequestValidator(),
                apiAccessAnomalyConfigStoreManager,
                new ApiAccessAnomalyConfigServiceRequestValidator(),
                abusePolicyConfigStoreManager,
                new AbusePolicyConfigRequestValidator()))
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
  @Tag("fraudPolicy")
  void testFraudPolicyCRUD() {
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

    FraudPolicy policy = FraudPolicy.newBuilder().setId(UUID_1).setName("updated").build();
    FieldMask output = FieldMaskUtils.generateFieldMask(policy);
    Assertions.assertEquals(2, output.getPathsCount());

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

    FraudPolicy disabledFraudPolicy =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub
                    .updateFraudPolicy(
                        UpdateFraudPolicyRequest.newBuilder()
                            .setFraudPolicyId(UUID_1)
                            .setFraudPolicy(
                                FraudPolicy.newBuilder()
                                    .setId(UUID_1)
                                    .setDisabled(true)
                                    .setName("disabled"))
                            .build())
                    .getFraudPolicy());

    assertEquals("disabled", disabledFraudPolicy.getName());
    fraudPolicyListResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.getFraudPolicyList(
                    GetFraudPolicyListRequest.getDefaultInstance()));
    assertEquals(0, fraudPolicyListResponse.getFraudPolicyListCount());
    fraudPolicyListResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.getFraudPolicyList(
                    GetFraudPolicyListRequest.newBuilder().setIncludeDisabled(true).build()));
    assertEquals(1, fraudPolicyListResponse.getFraudPolicyListCount());
    assertEquals(UUID_1, fraudPolicyListResponse.getFraudPolicyList(0).getId());

    GetFraudPolicyResponse fraudPolicyResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.getFraudPolicy(
                    GetFraudPolicyRequest.newBuilder()
                        .setFraudPolicyId(UUID_1)
                        .setIncludeDisabled(true)
                        .build()));
    assertEquals(UUID_1, fraudPolicyResponse.getFraudPolicy().getId());

    DeleteFraudPolicyResponse deleteFraudPolicyResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.deleteFraudPolicy(
                    DeleteFraudPolicyRequest.newBuilder().addFraudPolicyIdList(UUID_1).build()));
    assertEquals(1, deleteFraudPolicyResponse.getFraudPolicyListCount());
    assertEquals(UUID_1, deleteFraudPolicyResponse.getFraudPolicyList(0).getId());

    fraudPolicyListResponse =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub.getFraudPolicyList(
                    GetFraudPolicyListRequest.newBuilder().setIncludeDisabled(true).build()));
    assertEquals(0, fraudPolicyListResponse.getFraudPolicyListCount());
  }

  @Test
  @Tag("apiAccessAnomaly")
  void testCrud() {
    RequestContext requestContext = buildRequestContext();
    ApiAccessAnomalyConfig expected = new_config();

    ApiAccessAnomalyConfig created =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .createApiAccessAnomalyConfig(
                        CreateApiAccessAnomalyConfigRequest.newBuilder()
                            .addGroupedConfigs(
                                GroupedConfig.newBuilder()
                                    .setApiReference(
                                        expected.getGroupedConfigs(0).getApiReference())
                                    .setPre(expected.getGroupedConfigs(0).getPre())
                                    .setPost(expected.getGroupedConfigs(0).getPost())
                                    .setLookbackTimeWindow(
                                        expected.getGroupedConfigs(0).getLookbackTimeWindow())
                                    .build())
                            .build())
                    .getConfig());

    assertNotNull(created.getId());
    assertEquals(UUID_1, created.getId());
    String id = created.getId();

    ApiAccessAnomalyConfig toUpdate = update_config(id);

    ApiAccessAnomalyConfig updated =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .updateApiAccessAnomalyConfig(
                        UpdateApiAccessAnomalyConfigRequest.newBuilder()
                            .setConfig(toUpdate)
                            .build())
                    .getConfig());

    assertEquals(id, updated.getId());
    assertEquals(
        toUpdate.getGroupedConfigs(0).getPre().getApis(0).getApiReference().getApiId(),
        updated.getGroupedConfigs(0).getPre().getApis(0).getApiReference().getApiId());

    ApiAccessAnomalyConfig config =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .getApiAccessAnomalyConfig(
                        GetApiAccessAnomalyConfigRequest.newBuilder().setId(id).build())
                    .getConfig());

    assertEquals(id, config.getId());

    requestContext.call(
        () ->
            fraudPolicyConfigServiceBlockingStub.deleteApiAccessAnomalyConfig(
                DeleteApiAccessAnomalyConfigRequest.newBuilder().setId(id).build()));

    List<ApiAccessAnomalyConfig> configs =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .getApiAccessAnomalyConfigs(
                        GetApiAccessAnomalyConfigsRequest.newBuilder().build())
                    .getConfigsList());

    assertEquals(0, configs.size());
  }

  private ApiAccessAnomalyConfig new_config() {
    ApiCollection pre =
        ApiCollection.newBuilder()
            .addApis(
                APISpec.newBuilder()
                    .setApiReference(ApiReference.newBuilder().setApiId("api_1").build())
                    .setWeight(1)
                    .build())
            .build();
    ApiCollection post =
        ApiCollection.newBuilder()
            .addApis(
                APISpec.newBuilder()
                    .setApiReference(ApiReference.newBuilder().setApiId("api_2").build())
                    .setWeight(1)
                    .build())
            .build();

    CorrelationKey correlationKey =
        CorrelationKey.newBuilder().addAttributes("ip_address").addAttributes("user_agent").build();

    TimeWindow lookbackWindow =
        TimeWindow.newBuilder()
            .setRelativeTime(
                TimeObj.newBuilder().setUnit(TimeUnit.TIME_UNIT_MINUTES).setValue(15).build())
            .build();
    ApiAccessAnomalyConfig config =
        ApiAccessAnomalyConfig.newBuilder()
            .addGroupedConfigs(
                GroupedConfig.newBuilder()
                    .setApiReference(
                        ApiReference.newBuilder().setApiId("target_api_id_100").build())
                    .setCorrelationKey(correlationKey)
                    .setLookbackTimeWindow(lookbackWindow)
                    .setPre(pre)
                    .setPost(post)
                    .build())
            .build();
    return config;
  }

  private ApiAccessAnomalyConfig update_config(String id) {
    ApiCollection pre =
        ApiCollection.newBuilder()
            .addApis(
                APISpec.newBuilder()
                    .setApiReference(ApiReference.newBuilder().setApiId("api_updated_1").build())
                    .setWeight(1)
                    .build())
            .build();
    ApiCollection post =
        ApiCollection.newBuilder()
            .addApis(
                APISpec.newBuilder()
                    .setApiReference(ApiReference.newBuilder().setApiId("api_2").build())
                    .setWeight(1)
                    .build())
            .build();

    CorrelationKey correlationKey =
        CorrelationKey.newBuilder().addAttributes("ip_address").addAttributes("user_agent").build();

    TimeWindow lookbackWindow =
        TimeWindow.newBuilder()
            .setRelativeTime(
                TimeObj.newBuilder().setUnit(TimeUnit.TIME_UNIT_MINUTES).setValue(15).build())
            .build();
    ApiAccessAnomalyConfig config =
        ApiAccessAnomalyConfig.newBuilder()
            .setId(id)
            .addGroupedConfigs(
                GroupedConfig.newBuilder()
                    .setApiReference(
                        ApiReference.newBuilder().setApiId("target_api_id_100").build())
                    .setCorrelationKey(correlationKey)
                    .setLookbackTimeWindow(lookbackWindow)
                    .setPre(pre)
                    .setPost(post)
                    .build())
            .build();
    return config;
  }

  @Test
  @Tag("abusePolicy")
  void testCreateAbusePolicy() {
    RequestContext requestContext = buildRequestContext();

    ai.traceable.fraud.policy.config.service.v1.AbusePolicy created =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .createAbusePolicy(
                        ai.traceable.fraud.policy.config.service.v1.CreateAbusePolicyRequest
                            .newBuilder()
                            .setData(createValidAbusePolicyData())
                            .build())
                    .getPolicy());

    assertNotNull(created.getId());
    assertEquals(UUID_1, created.getId());
    assertEquals("Test Abuse Policy", created.getData().getName());
    assertTrue(created.getData().getEnabled());
    assertEquals(0, created.getData().getVersion());
    assertEquals(
        ai.traceable.fraud.policy.config.service.v1.AbuseRiskSeverity.ABUSE_RISK_SEVERITY_HIGH,
        created.getData().getSeverity());
  }

  @Test
  @Tag("abusePolicy")
  void testUpdateAbusePolicy() {
    RequestContext requestContext = buildRequestContext();

    // Create first
    ai.traceable.fraud.policy.config.service.v1.AbusePolicy created =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .createAbusePolicy(
                        ai.traceable.fraud.policy.config.service.v1.CreateAbusePolicyRequest
                            .newBuilder()
                            .setData(createValidAbusePolicyData())
                            .build())
                    .getPolicy());

    String policyId = created.getId();

    // Update
    ai.traceable.fraud.policy.config.service.v1.AbusePolicy updated =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .updateAbusePolicy(
                        UpdateAbusePolicyRequest.newBuilder()
                            .setPolicyId(policyId)
                            .setData(
                                created.getData().toBuilder()
                                    .setName("Updated Abuse Policy")
                                    .setEnabled(false)
                                    .setVersion(0))
                            .build())
                    .getPolicy());

    assertEquals(policyId, updated.getId());
    assertEquals("Updated Abuse Policy", updated.getData().getName());
    assertFalse(updated.getData().getEnabled());
    assertEquals(1, updated.getData().getVersion());
  }

  @Test
  @Tag("abusePolicy")
  void testGetAbusePolicy() {
    RequestContext requestContext = buildRequestContext();

    // Create first
    ai.traceable.fraud.policy.config.service.v1.AbusePolicy created =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .createAbusePolicy(
                        ai.traceable.fraud.policy.config.service.v1.CreateAbusePolicyRequest
                            .newBuilder()
                            .setData(createValidAbusePolicyData())
                            .build())
                    .getPolicy());

    String policyId = created.getId();

    // Get single policy
    ai.traceable.fraud.policy.config.service.v1.AbusePolicy fetched =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .getAbusePolicy(
                        ai.traceable.fraud.policy.config.service.v1.GetAbusePolicyRequest
                            .newBuilder()
                            .setPolicyId(policyId)
                            .build())
                    .getPolicy());

    assertEquals(policyId, fetched.getId());
    assertEquals("Test Abuse Policy", fetched.getData().getName());
    assertEquals(created.getData(), fetched.getData());
  }

  @Test
  @Tag("abusePolicy")
  void testGetAbusePolicies() {
    RequestContext requestContext = buildRequestContext();

    // Create first policy
    ai.traceable.fraud.policy.config.service.v1.AbusePolicy created1 =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .createAbusePolicy(
                        ai.traceable.fraud.policy.config.service.v1.CreateAbusePolicyRequest
                            .newBuilder()
                            .setData(createValidAbusePolicyData())
                            .build())
                    .getPolicy());

    // Get all policies
    java.util.List<ai.traceable.fraud.policy.config.service.v1.AbusePolicy> policies =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .getAbusePolicies(
                        ai.traceable.fraud.policy.config.service.v1.GetAbusePoliciesRequest
                            .newBuilder()
                            .build())
                    .getPoliciesList());

    assertEquals(1, policies.size());
    assertEquals(created1.getId(), policies.get(0).getId());
    assertEquals("Test Abuse Policy", policies.get(0).getData().getName());
  }

  @Test
  @Tag("abusePolicy")
  void testDeleteAbusePolicy() {
    RequestContext requestContext = buildRequestContext();

    // Create first
    ai.traceable.fraud.policy.config.service.v1.AbusePolicy created =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .createAbusePolicy(
                        ai.traceable.fraud.policy.config.service.v1.CreateAbusePolicyRequest
                            .newBuilder()
                            .setData(createValidAbusePolicyData())
                            .build())
                    .getPolicy());

    String policyId = created.getId();

    // Delete
    ai.traceable.fraud.policy.config.service.v1.DeleteAbusePolicyResponse deleteResponse =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub.deleteAbusePolicy(
                    ai.traceable.fraud.policy.config.service.v1.DeleteAbusePolicyRequest
                        .newBuilder()
                        .addPolicyIds(policyId)
                        .build()));

    assertEquals(1, deleteResponse.getPoliciesCount());
    assertEquals(policyId, deleteResponse.getPolicies(0).getId());

    // Verify deletion
    java.util.List<ai.traceable.fraud.policy.config.service.v1.AbusePolicy> policies =
        requestContext.call(
            () ->
                fraudPolicyConfigServiceBlockingStub
                    .getAbusePolicies(
                        ai.traceable.fraud.policy.config.service.v1.GetAbusePoliciesRequest
                            .newBuilder()
                            .build())
                    .getPoliciesList());

    assertEquals(0, policies.size());
  }

  private ai.traceable.fraud.policy.config.service.v1.AbusePolicyData createValidAbusePolicyData() {
    return ai.traceable.fraud.policy.config.service.v1.AbusePolicyData.newBuilder()
        .setName("Test Abuse Policy")
        .setScope(
            ai.traceable.fraud.policy.config.service.v1.AbusePolicyScope.newBuilder()
                .setEnvironmentScope(
                    ai.traceable.fraud.policy.config.service.v1.AbuseEnvironmentScope.newBuilder()
                        .addEnvironmentIds("env1")
                        .build())
                .setApiScope(
                    ai.traceable.fraud.policy.config.service.v1.AbuseApiScope.newBuilder()
                        .setApiIds(
                            ai.traceable.fraud.policy.config.service.v1.AbuseApiIds.newBuilder()
                                .addIds("api1")
                                .build())
                        .build())
                .build())
        .setEnabled(true)
        .setSeverity(
            ai.traceable.fraud.policy.config.service.v1.AbuseRiskSeverity.ABUSE_RISK_SEVERITY_HIGH)
        .setAction(
            ai.traceable.fraud.policy.config.service.v1.AbuseActionConfig.newBuilder()
                .setActionType(
                    ai.traceable.fraud.policy.config.service.v1.AbuseActionType
                        .ABUSE_ACTION_TYPE_ALERT)
                .build())
        .setSimpleAggregationTemplate(
            ai.traceable.fraud.policy.config.service.v1.AbuseSimpleAggregationTemplateConfig
                .newBuilder()
                .setAggregation(
                    ai.traceable.fraud.policy.config.service.v1.AbuseAggregationConfig.newBuilder()
                        .setFunction(
                            ai.traceable.fraud.policy.config.service.v1.AbuseAggregationFunction
                                .ABUSE_AGGREGATION_FUNCTION_COUNT)
                        .setDerivedEntityId("request_count")
                        .build())
                .setThreshold(
                    ai.traceable.fraud.policy.config.service.v1.AbuseThresholdConfig.newBuilder()
                        .setOperator(
                            ai.traceable.fraud.policy.config.service.v1.AbuseThresholdOperator
                                .ABUSE_THRESHOLD_OPERATOR_GREATER_THAN)
                        .setValue(100)
                        .build())
                .setTimeWindow(
                    ai.traceable.fraud.policy.config.service.v1.AbuseTimeWindow.newBuilder()
                        .setLookbackDuration(
                            com.google.protobuf.Duration.newBuilder().setSeconds(3600).build())
                        .build())
                .build())
        .setMessageFormat("Abuse detected: {message}")
        .build();
  }

  private static RequestContext buildRequestContext() {
    return RequestContext.forTenantId("t1");
  }
}
