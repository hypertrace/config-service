package ai.traceable.fraud.policy.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.config.proto.utils.FieldMaskUtils;
import ai.traceable.config.utils.UuidGenerator;
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
import ai.traceable.fraud.policy.config.service.v1.EntityGraphBasedFraudRule;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicy;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyConfigServiceGrpc;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyRule;
import ai.traceable.fraud.policy.config.service.v1.FraudRule;
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
import ai.traceable.fraud.policy.config.service.v1.UpdateApiAccessAnomalyConfigRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpsertFraudPolicyRequest;
import ai.traceable.fraud.query.model.v1.EntityGraphDataQuery;
import com.google.protobuf.FieldMask;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class FraudPolicyConfigServiceImplTest {

  private static final String UUID_1 = "uuid-1";

  private FraudPolicyConfigServiceGrpc.FraudPolicyConfigServiceBlockingStub
      fraudPolicyConfigServiceBlockingStub;
  private FraudPolicyConfigStoreManager storeManager;
  private ApiAccessAnomalyConfigStoreManager apiAccessAnomalyConfigStoreManager;

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
    this.mockGenericConfigService
        .addService(
            new FraudPolicyConfigServiceImpl(storeManager, apiAccessAnomalyConfigStoreManager))
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
    String validSqlQuery = createValidSqlQuery();

    FraudPolicy createdFraudPolicy =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub
                    .createFraudPolicy(
                        CreateFraudPolicyRequest.newBuilder()
                            .setFraudPolicy(
                                FraudPolicy.newBuilder()
                                    .setId(UUID_1)
                                    .setName("created")
                                    .setFraudPolicyRule(
                                        FraudPolicyRule.newBuilder()
                                            .setFraudRule(
                                                FraudRule.newBuilder()
                                                    .setEntityGraphFraudRule(
                                                        EntityGraphBasedFraudRule.newBuilder()
                                                            .setEntityGraphDataQuery(
                                                                EntityGraphDataQuery.newBuilder()
                                                                    .setRawSqlQuery(validSqlQuery)
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .getFraudPolicy());

    assertEquals(UUID_1, createdFraudPolicy.getId());

    FraudPolicy policy =
        FraudPolicy.newBuilder()
            .setId(UUID_1)
            .setName("updated")
            .setFraudPolicyRule(
                FraudPolicyRule.newBuilder()
                    .setFraudRule(
                        FraudRule.newBuilder()
                            .setEntityGraphFraudRule(
                                EntityGraphBasedFraudRule.newBuilder()
                                    .setEntityGraphDataQuery(
                                        EntityGraphDataQuery.newBuilder()
                                            .setRawSqlQuery(validSqlQuery)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    FieldMask output = FieldMaskUtils.generateFieldMask(policy);
    // Verify that the field mask includes at least the name and fraud_policy_rule fields
    assertTrue(output.getPathsCount() >= 2);
    assertTrue(output.getPathsList().contains("name"));
    assertTrue(output.getPathsList().contains("fraud_policy_rule"));

    FraudPolicy updatedFraudPolicy =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub
                    .updateFraudPolicy(
                        UpdateFraudPolicyRequest.newBuilder()
                            .setFraudPolicyId(UUID_1)
                            .setFraudPolicy(
                                FraudPolicy.newBuilder().setId(UUID_1).setName("updated"))
                            .setCurrentVersion(createdFraudPolicy.getVersion())
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
                            .setCurrentVersion(updatedFraudPolicy.getVersion())
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
  @Tag("fraudPolicy")
  void testUpsertFraudPolicy() {
    RequestContext requestContext = buildRequestContext();
    String validSqlQuery = createValidSqlQuery();

    FraudPolicy upsertedFraudPolicy =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub
                    .upsertFraudPolicy(
                        UpsertFraudPolicyRequest.newBuilder()
                            .setFraudPolicy(
                                FraudPolicy.newBuilder()
                                    .setId(UUID_1)
                                    .setName("upserted")
                                    .setFraudPolicyRule(
                                        FraudPolicyRule.newBuilder()
                                            .setFraudRule(
                                                FraudRule.newBuilder()
                                                    .setEntityGraphFraudRule(
                                                        EntityGraphBasedFraudRule.newBuilder()
                                                            .setEntityGraphDataQuery(
                                                                EntityGraphDataQuery.newBuilder()
                                                                    .setRawSqlQuery(validSqlQuery)
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .getFraudPolicy());

    assertEquals(UUID_1, upsertedFraudPolicy.getId());
    assertEquals("upserted", upsertedFraudPolicy.getName());
  }

  @Test
  @Tag("fraudPolicy")
  void testUpdateFraudPolicyWithSqlQuery() {
    RequestContext requestContext = buildRequestContext();
    String validSqlQuery = createValidSqlQuery();

    // First create a policy
    FraudPolicy createdFraudPolicy =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub
                    .createFraudPolicy(
                        CreateFraudPolicyRequest.newBuilder()
                            .setFraudPolicy(
                                FraudPolicy.newBuilder()
                                    .setId(UUID_1)
                                    .setName("created")
                                    .setFraudPolicyRule(
                                        FraudPolicyRule.newBuilder()
                                            .setFraudRule(
                                                FraudRule.newBuilder()
                                                    .setEntityGraphFraudRule(
                                                        EntityGraphBasedFraudRule.newBuilder()
                                                            .setEntityGraphDataQuery(
                                                                EntityGraphDataQuery.newBuilder()
                                                                    .setRawSqlQuery(validSqlQuery)
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .getFraudPolicy());

    assertEquals(UUID_1, createdFraudPolicy.getId());

    // Update with a new SQL query
    String updatedSqlQuery = createValidSqlQuery() + " AND additional_condition = 'test'";
    FraudPolicy updatedFraudPolicy =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub
                    .updateFraudPolicy(
                        UpdateFraudPolicyRequest.newBuilder()
                            .setFraudPolicyId(UUID_1)
                            .setFraudPolicy(
                                FraudPolicy.newBuilder()
                                    .setId(UUID_1)
                                    .setName("updated")
                                    .setFraudPolicyRule(
                                        FraudPolicyRule.newBuilder()
                                            .setFraudRule(
                                                FraudRule.newBuilder()
                                                    .setEntityGraphFraudRule(
                                                        EntityGraphBasedFraudRule.newBuilder()
                                                            .setEntityGraphDataQuery(
                                                                EntityGraphDataQuery.newBuilder()
                                                                    .setRawSqlQuery(updatedSqlQuery)
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build()))
                            .setCurrentVersion(createdFraudPolicy.getVersion())
                            .build())
                    .getFraudPolicy());

    assertEquals("updated", updatedFraudPolicy.getName());
  }

  @Test
  @Tag("fraudPolicy")
  void testCreateFraudPolicyWithInvalidSqlQuery() {
    RequestContext requestContext = buildRequestContext();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                requestContext.call(
                    () ->
                        this.fraudPolicyConfigServiceBlockingStub
                            .createFraudPolicy(
                                CreateFraudPolicyRequest.newBuilder()
                                    .setFraudPolicy(
                                        FraudPolicy.newBuilder()
                                            .setId(UUID_1)
                                            .setName("invalid")
                                            .setFraudPolicyRule(
                                                FraudPolicyRule.newBuilder()
                                                    .setFraudRule(
                                                        FraudRule.newBuilder()
                                                            .setEntityGraphFraudRule(
                                                                EntityGraphBasedFraudRule
                                                                    .newBuilder()
                                                                    .setEntityGraphDataQuery(
                                                                        EntityGraphDataQuery
                                                                            .newBuilder()
                                                                            .setRawSqlQuery(
                                                                                "SELECT * FROM table")
                                                                            .build())
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .getFraudPolicy()));

    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, exception.getStatus().getCode());
  }

  @Test
  @Tag("fraudPolicy")
  void testUpsertFraudPolicyWithInvalidSqlQuery() {
    RequestContext requestContext = buildRequestContext();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                requestContext.call(
                    () ->
                        this.fraudPolicyConfigServiceBlockingStub
                            .upsertFraudPolicy(
                                UpsertFraudPolicyRequest.newBuilder()
                                    .setFraudPolicy(
                                        FraudPolicy.newBuilder()
                                            .setId(UUID_1)
                                            .setName("invalid")
                                            .setFraudPolicyRule(
                                                FraudPolicyRule.newBuilder()
                                                    .setFraudRule(
                                                        FraudRule.newBuilder()
                                                            .setEntityGraphFraudRule(
                                                                EntityGraphBasedFraudRule
                                                                    .newBuilder()
                                                                    .setEntityGraphDataQuery(
                                                                        EntityGraphDataQuery
                                                                            .newBuilder()
                                                                            .setRawSqlQuery(
                                                                                "SELECT * FROM table")
                                                                            .build())
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .getFraudPolicy()));

    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, exception.getStatus().getCode());
  }

  @Test
  @Tag("fraudPolicy")
  void testUpdateFraudPolicyWithInvalidSqlQuery() {
    RequestContext requestContext = buildRequestContext();
    String validSqlQuery = createValidSqlQuery();

    // First create a policy with valid SQL
    FraudPolicy createdFraudPolicy =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub
                    .createFraudPolicy(
                        CreateFraudPolicyRequest.newBuilder()
                            .setFraudPolicy(
                                FraudPolicy.newBuilder()
                                    .setId(UUID_1)
                                    .setName("created")
                                    .setFraudPolicyRule(
                                        FraudPolicyRule.newBuilder()
                                            .setFraudRule(
                                                FraudRule.newBuilder()
                                                    .setEntityGraphFraudRule(
                                                        EntityGraphBasedFraudRule.newBuilder()
                                                            .setEntityGraphDataQuery(
                                                                EntityGraphDataQuery.newBuilder()
                                                                    .setRawSqlQuery(validSqlQuery)
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .getFraudPolicy());

    // Note: Update method doesn't validate SQL queries, so update with invalid SQL should succeed
    // This test verifies that update doesn't validate SQL (as per current implementation)
    FraudPolicy updatedFraudPolicy =
        requestContext.call(
            () ->
                this.fraudPolicyConfigServiceBlockingStub
                    .updateFraudPolicy(
                        UpdateFraudPolicyRequest.newBuilder()
                            .setFraudPolicyId(UUID_1)
                            .setFraudPolicy(
                                FraudPolicy.newBuilder()
                                    .setId(UUID_1)
                                    .setName("updated")
                                    .setFraudPolicyRule(
                                        FraudPolicyRule.newBuilder()
                                            .setFraudRule(
                                                FraudRule.newBuilder()
                                                    .setEntityGraphFraudRule(
                                                        EntityGraphBasedFraudRule.newBuilder()
                                                            .setEntityGraphDataQuery(
                                                                EntityGraphDataQuery.newBuilder()
                                                                    .setRawSqlQuery(
                                                                        "SELECT * FROM table")
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build()))
                            .setCurrentVersion(createdFraudPolicy.getVersion())
                            .build())
                    .getFraudPolicy());

    assertEquals("updated", updatedFraudPolicy.getName());
  }

  @Test
  @Tag("fraudPolicy")
  void testCreateFraudPolicyWithEmptySqlQuery() {
    RequestContext requestContext = buildRequestContext();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                requestContext.call(
                    () ->
                        this.fraudPolicyConfigServiceBlockingStub
                            .createFraudPolicy(
                                CreateFraudPolicyRequest.newBuilder()
                                    .setFraudPolicy(
                                        FraudPolicy.newBuilder()
                                            .setId(UUID_1)
                                            .setName("empty_sql")
                                            .setFraudPolicyRule(
                                                FraudPolicyRule.newBuilder()
                                                    .setFraudRule(
                                                        FraudRule.newBuilder()
                                                            .setEntityGraphFraudRule(
                                                                EntityGraphBasedFraudRule
                                                                    .newBuilder()
                                                                    .setEntityGraphDataQuery(
                                                                        EntityGraphDataQuery
                                                                            .newBuilder()
                                                                            .setRawSqlQuery("")
                                                                            .build())
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .getFraudPolicy()));

    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
    assertEquals("SQL query cannot be null or empty", exception.getStatus().getDescription());
  }

  @Test
  @Tag("fraudPolicy")
  void testUpsertFraudPolicyWithEmptySqlQuery() {
    RequestContext requestContext = buildRequestContext();

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                requestContext.call(
                    () ->
                        this.fraudPolicyConfigServiceBlockingStub
                            .upsertFraudPolicy(
                                UpsertFraudPolicyRequest.newBuilder()
                                    .setFraudPolicy(
                                        FraudPolicy.newBuilder()
                                            .setId(UUID_1)
                                            .setName("empty_sql")
                                            .setFraudPolicyRule(
                                                FraudPolicyRule.newBuilder()
                                                    .setFraudRule(
                                                        FraudRule.newBuilder()
                                                            .setEntityGraphFraudRule(
                                                                EntityGraphBasedFraudRule
                                                                    .newBuilder()
                                                                    .setEntityGraphDataQuery(
                                                                        EntityGraphDataQuery
                                                                            .newBuilder()
                                                                            .setRawSqlQuery("")
                                                                            .build())
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .getFraudPolicy()));

    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
    assertEquals("SQL query cannot be null or empty", exception.getStatus().getDescription());
  }

  @Test
  @Tag("fraudPolicy")
  void testCreateFraudPolicyWithMissingRequiredColumns() {
    RequestContext requestContext = buildRequestContext();

    // SQL query missing some required columns
    String sqlWithMissingColumns = "SELECT primary_entity_type, primary_entity_id FROM fraud_table";

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                requestContext.call(
                    () ->
                        this.fraudPolicyConfigServiceBlockingStub
                            .createFraudPolicy(
                                CreateFraudPolicyRequest.newBuilder()
                                    .setFraudPolicy(
                                        FraudPolicy.newBuilder()
                                            .setId(UUID_1)
                                            .setName("missing_columns")
                                            .setFraudPolicyRule(
                                                FraudPolicyRule.newBuilder()
                                                    .setFraudRule(
                                                        FraudRule.newBuilder()
                                                            .setEntityGraphFraudRule(
                                                                EntityGraphBasedFraudRule
                                                                    .newBuilder()
                                                                    .setEntityGraphDataQuery(
                                                                        EntityGraphDataQuery
                                                                            .newBuilder()
                                                                            .setRawSqlQuery(
                                                                                sqlWithMissingColumns)
                                                                            .build())
                                                                    .build())
                                                            .build())
                                                    .build())
                                            .build())
                                    .build())
                            .getFraudPolicy()));

    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, exception.getStatus().getCode());
    String description = exception.getStatus().getDescription();
    assertNotNull(description);
    // Verify the error message mentions missing columns
    assertTrue(description.contains("missing required columns"));
  }

  @Test
  @Tag("apiAccessAnomaly")
  public void testCrud() {
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

  private static RequestContext buildRequestContext() {
    return RequestContext.forTenantId("t1");
  }

  private String createValidSqlQuery() {
    // Create a SQL query that includes all required columns:
    // primary_entity_type, primary_entity_id, primary_entity_name, environment, target, target_type
    return "SELECT primary_entity_type, primary_entity_id, primary_entity_name, environment, target, target_type FROM fraud_table WHERE condition = 'value'";
  }
}
