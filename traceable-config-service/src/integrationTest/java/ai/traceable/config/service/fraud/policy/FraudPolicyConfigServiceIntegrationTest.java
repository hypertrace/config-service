package ai.traceable.config.service.fraud.policy;

import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.config.service.fraud.ResourceUtils;
import ai.traceable.fraud.policy.config.service.v1.CreateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.EntityGraphBasedFraudRule;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicy;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyConfigServiceGrpc;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyList;
import ai.traceable.fraud.policy.config.service.v1.FraudPolicyRule;
import ai.traceable.fraud.policy.config.service.v1.FraudRule;
import ai.traceable.fraud.policy.config.service.v1.GetFraudPolicyListRequest;
import ai.traceable.fraud.policy.config.service.v1.UpdateFraudPolicyRequest;
import ai.traceable.fraud.policy.config.service.v1.UpsertFraudPolicyRequest;
import ai.traceable.fraud.query.model.v1.EntityGraphDataQuery;
import ai.traceable.fraud.query.model.v1.EntityMetricDataQuery;
import ai.traceable.fraud.query.model.v1.MetricDataQuery;
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

    // Update SQL query to include all required columns for validation
    fraudPolicy = updateSqlQueryWithRequiredColumns(fraudPolicy);

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

  @Test
  public void testCreateFraudPolicyWithInvalidSqlQuery() {
    CreateFraudPolicyRequest createFraudPolicyRequest =
        CreateFraudPolicyRequest.newBuilder()
            .setFraudPolicy(
                FraudPolicy.newBuilder()
                    .setName("invalid")
                    .setFraudPolicyRule(
                        FraudPolicyRule.newBuilder()
                            .setFraudRule(
                                FraudRule.newBuilder()
                                    .setEntityGraphFraudRule(
                                        EntityGraphBasedFraudRule.newBuilder()
                                            .setEntityGraphDataQuery(
                                                EntityGraphDataQuery.newBuilder()
                                                    .setRawSqlQuery("SELECT * FROM table")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    try {
      RequestContext.forTenantId(TENANT_ID)
          .call(() -> serviceStub.createFraudPolicy(createFraudPolicyRequest));
      Assertions.fail("Expected create to fail with invalid SQL query");
    } catch (StatusRuntimeException e) {
      Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }
  }

  @Test
  public void testUpsertFraudPolicyWithInvalidSqlQuery() {
    UpsertFraudPolicyRequest upsertFraudPolicyRequest =
        UpsertFraudPolicyRequest.newBuilder()
            .setFraudPolicy(
                FraudPolicy.newBuilder()
                    .setName("invalid")
                    .setFraudPolicyRule(
                        FraudPolicyRule.newBuilder()
                            .setFraudRule(
                                FraudRule.newBuilder()
                                    .setEntityGraphFraudRule(
                                        EntityGraphBasedFraudRule.newBuilder()
                                            .setEntityGraphDataQuery(
                                                EntityGraphDataQuery.newBuilder()
                                                    .setRawSqlQuery("SELECT * FROM table")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    try {
      RequestContext.forTenantId(TENANT_ID)
          .call(() -> serviceStub.upsertFraudPolicy(upsertFraudPolicyRequest));
      Assertions.fail("Expected upsert to fail with invalid SQL query");
    } catch (StatusRuntimeException e) {
      Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }
  }

  @Test
  public void testCreateFraudPolicyWithEmptySqlQuery() {
    CreateFraudPolicyRequest createFraudPolicyRequest =
        CreateFraudPolicyRequest.newBuilder()
            .setFraudPolicy(
                FraudPolicy.newBuilder()
                    .setName("empty_sql")
                    .setFraudPolicyRule(
                        FraudPolicyRule.newBuilder()
                            .setFraudRule(
                                FraudRule.newBuilder()
                                    .setEntityGraphFraudRule(
                                        EntityGraphBasedFraudRule.newBuilder()
                                            .setEntityGraphDataQuery(
                                                EntityGraphDataQuery.newBuilder()
                                                    .setRawSqlQuery("")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    try {
      RequestContext.forTenantId(TENANT_ID)
          .call(() -> serviceStub.createFraudPolicy(createFraudPolicyRequest));
      Assertions.fail("Expected create to fail with empty SQL query");
    } catch (StatusRuntimeException e) {
      Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
      Assertions.assertEquals("SQL query cannot be null or empty", e.getStatus().getDescription());
    }
  }

  @Test
  public void testUpsertFraudPolicyWithEmptySqlQuery() {
    UpsertFraudPolicyRequest upsertFraudPolicyRequest =
        UpsertFraudPolicyRequest.newBuilder()
            .setFraudPolicy(
                FraudPolicy.newBuilder()
                    .setName("empty_sql")
                    .setFraudPolicyRule(
                        FraudPolicyRule.newBuilder()
                            .setFraudRule(
                                FraudRule.newBuilder()
                                    .setEntityGraphFraudRule(
                                        EntityGraphBasedFraudRule.newBuilder()
                                            .setEntityGraphDataQuery(
                                                EntityGraphDataQuery.newBuilder()
                                                    .setRawSqlQuery("")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    try {
      RequestContext.forTenantId(TENANT_ID)
          .call(() -> serviceStub.upsertFraudPolicy(upsertFraudPolicyRequest));
      Assertions.fail("Expected upsert to fail with empty SQL query");
    } catch (StatusRuntimeException e) {
      Assertions.assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
      Assertions.assertEquals("SQL query cannot be null or empty", e.getStatus().getDescription());
    }
  }

  @Test
  public void testCreateFraudPolicyWithMissingRequiredColumns() {
    // SQL query missing some required columns
    String sqlWithMissingColumns = "SELECT primary_entity_type, primary_entity_id FROM fraud_table";

    CreateFraudPolicyRequest createFraudPolicyRequest =
        CreateFraudPolicyRequest.newBuilder()
            .setFraudPolicy(
                FraudPolicy.newBuilder()
                    .setName("missing_columns")
                    .setFraudPolicyRule(
                        FraudPolicyRule.newBuilder()
                            .setFraudRule(
                                FraudRule.newBuilder()
                                    .setEntityGraphFraudRule(
                                        EntityGraphBasedFraudRule.newBuilder()
                                            .setEntityGraphDataQuery(
                                                EntityGraphDataQuery.newBuilder()
                                                    .setRawSqlQuery(sqlWithMissingColumns)
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    try {
      RequestContext.forTenantId(TENANT_ID)
          .call(() -> serviceStub.createFraudPolicy(createFraudPolicyRequest));
      Assertions.fail("Expected create to fail with missing required columns");
    } catch (StatusRuntimeException e) {
      Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
      String description = e.getStatus().getDescription();
      Assertions.assertNotNull(description);
      // Verify the error message mentions missing columns
      Assertions.assertTrue(description.contains("missing required columns"));
    }
  }

  private FraudPolicy updateSqlQueryWithRequiredColumns(FraudPolicy fraudPolicy) {
    if (!fraudPolicy.hasFraudPolicyRule() || !fraudPolicy.getFraudPolicyRule().hasFraudRule()) {
      return fraudPolicy;
    }

    String validSqlQuery = createValidSqlQuery();
    var fraudRule = fraudPolicy.getFraudPolicyRule().getFraudRule();
    var fraudPolicyRuleBuilder = fraudPolicy.getFraudPolicyRule().toBuilder();
    var fraudRuleBuilder = fraudRule.toBuilder();

    // Handle EntityGraphFraudRule
    if (fraudRule.hasEntityGraphFraudRule()) {
      var entityGraphFraudRule = fraudRule.getEntityGraphFraudRule();
      if (entityGraphFraudRule.hasEntityGraphDataQuery()) {
        EntityGraphDataQuery updatedQuery =
            entityGraphFraudRule.getEntityGraphDataQuery().toBuilder()
                .setRawSqlQuery(validSqlQuery)
                .build();
        fraudRuleBuilder.setEntityGraphFraudRule(
            entityGraphFraudRule.toBuilder().setEntityGraphDataQuery(updatedQuery).build());
      }
    }
    // Handle EntityMetricFraudRule
    else if (fraudRule.hasEntityMetricFraudRule()) {
      var entityMetricFraudRule = fraudRule.getEntityMetricFraudRule();
      if (entityMetricFraudRule.hasEntityMetricDataQuery()) {
        EntityMetricDataQuery updatedQuery =
            entityMetricFraudRule.getEntityMetricDataQuery().toBuilder()
                .setRawSqlQuery(validSqlQuery)
                .build();
        fraudRuleBuilder.setEntityMetricFraudRule(
            entityMetricFraudRule.toBuilder().setEntityMetricDataQuery(updatedQuery).build());
      }
    }
    // Handle MetricFraudRule
    else if (fraudRule.hasMetricFraudRule()) {
      var metricFraudRule = fraudRule.getMetricFraudRule();
      if (metricFraudRule.hasMetricDataQuery()) {
        MetricDataQuery updatedQuery =
            metricFraudRule.getMetricDataQuery().toBuilder().setRawSqlQuery(validSqlQuery).build();
        fraudRuleBuilder.setMetricFraudRule(
            metricFraudRule.toBuilder().setMetricDataQuery(updatedQuery).build());
      }
    }

    return fraudPolicy.toBuilder()
        .setFraudPolicyRule(fraudPolicyRuleBuilder.setFraudRule(fraudRuleBuilder.build()).build())
        .build();
  }

  private String createValidSqlQuery() {
    // Create a SQL query that includes all required columns:
    // primary_entity_type, primary_entity_id, primary_entity_name, environment, target, target_type
    return "SELECT primary_entity_type, primary_entity_id, primary_entity_name, environment, target, target_type FROM fraud_table WHERE condition = 'value'";
  }
}
