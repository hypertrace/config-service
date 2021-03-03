package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.ratelimiting.config.service.v1.CreateRateLimitingRuleConfig;
import ai.traceable.ratelimiting.config.service.v1.CreateRuleConfigRequest;
import ai.traceable.ratelimiting.config.service.v1.CreateRuleRateLimitedEntityAssociationRequest;
import ai.traceable.ratelimiting.config.service.v1.DeleteRuleConfigRequest;
import ai.traceable.ratelimiting.config.service.v1.DeleteRuleRateLimitedEntityAssociationRequest;
import ai.traceable.ratelimiting.config.service.v1.GetAllRateLimitingRulesRequest;
import ai.traceable.ratelimiting.config.service.v1.GetRateLimitingConfigsForEntityRequest;
import ai.traceable.ratelimiting.config.service.v1.RateLimitedEntity;
import ai.traceable.ratelimiting.config.service.v1.RateLimitedEntityType;
import ai.traceable.ratelimiting.config.service.v1.RateLimitingConfigServiceGrpc;
import ai.traceable.ratelimiting.config.service.v1.RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub;
import ai.traceable.ratelimiting.config.service.v1.RateLimitingRuleConfig;
import ai.traceable.ratelimiting.config.service.v1.RateLimitingRuleWithRateLimitedEntities;
import ai.traceable.ratelimiting.config.service.v1.RuleViolationAction;
import ai.traceable.ratelimiting.config.service.v1.UpdateRuleConfigRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test for RateLimitingConfigService */
public class RateLimitingConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {

  private static RateLimitingConfigServiceBlockingStub rateLimitingConfigServiceStub;

  @BeforeAll
  static void init() {
    rateLimitingConfigServiceStub =
        RateLimitingConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testRateLimitingConfig() {
    // Create rule1
    CreateRateLimitingRuleConfig createRateLimitingRuleConfigOne =
        CreateRateLimitingRuleConfig.newBuilder()
            .setRuleName("rule1")
            .setDescription("this is rule 1")
            .setMaxCallCountAllowed(10L)
            .setMaxCallCountDurationMillis(10000L)
            .setRuleViolationAction(RuleViolationAction.RULE_VIOLATION_ACTION_SUSPEND)
            .setSuspendDurationMillis(1000L)
            .build();
    RateLimitingRuleConfig createdRateLimitRuleConfigOne =
        createRateLimitingRuleConfig(createRateLimitingRuleConfigOne);
    String ruleId1 = createdRateLimitRuleConfigOne.getRuleId();

    // Create rule2
    CreateRateLimitingRuleConfig createRateLimitingRuleConfigTwo =
        CreateRateLimitingRuleConfig.newBuilder()
            .setRuleName("rule2")
            .setDescription("this is rule 1")
            .setMaxCallCountAllowed(10L)
            .setMaxCallCountDurationMillis(100000L)
            .setRuleViolationAction(RuleViolationAction.RULE_VIOLATION_ACTION_SUSPEND)
            .setSuspendDurationMillis(1000L)
            .build();
    String ruleId2 = createRateLimitingRuleConfig(createRateLimitingRuleConfigTwo).getRuleId();

    List<RateLimitingRuleWithRateLimitedEntities> rulesWithEntities = getAllRateLimitingRule();

    // Assert two rules created
    assertEquals(2, rulesWithEntities.size());

    // Associate entity1 with rule1.
    RateLimitedEntity entityOne =
        RateLimitedEntity.newBuilder()
            .setEntityId("entity1")
            .setEntityType(RateLimitedEntityType.RATE_LIMITED_ENTITY_TYPE_API)
            .build();

    CreateRuleRateLimitedEntityAssociationRequest createAssociationRequestOne =
        CreateRuleRateLimitedEntityAssociationRequest.newBuilder()
            .setRuleId(ruleId1)
            .setEntity(entityOne)
            .build();

    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            rateLimitingConfigServiceStub.createRuleRateLimitedEntityAssociation(
                createAssociationRequestOne));

    // Associate entity2 with rule1.
    RateLimitedEntity entityTwo =
        RateLimitedEntity.newBuilder()
            .setEntityId("entity2")
            .setEntityType(RateLimitedEntityType.RATE_LIMITED_ENTITY_TYPE_API)
            .build();

    CreateRuleRateLimitedEntityAssociationRequest createAssociationRequestTwo =
        CreateRuleRateLimitedEntityAssociationRequest.newBuilder()
            .setRuleId(ruleId1)
            .setEntity(entityTwo)
            .build();

    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            rateLimitingConfigServiceStub.createRuleRateLimitedEntityAssociation(
                createAssociationRequestTwo));

    // Associate entity3 with rule2.
    RateLimitedEntity entityThree =
        RateLimitedEntity.newBuilder()
            .setEntityId("entity3")
            .setEntityType(RateLimitedEntityType.RATE_LIMITED_ENTITY_TYPE_API)
            .build();

    CreateRuleRateLimitedEntityAssociationRequest createAssociationRequestThree =
        CreateRuleRateLimitedEntityAssociationRequest.newBuilder()
            .setRuleId(ruleId2)
            .setEntity(entityThree)
            .build();

    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            rateLimitingConfigServiceStub.createRuleRateLimitedEntityAssociation(
                createAssociationRequestThree));

    // After creating all the associations
    // rule1 -> entity1, entity2
    // rule2 -> entity3
    rulesWithEntities = getAllRateLimitingRule();
    assertRuleAssociations(rulesWithEntities.get(0), ruleId1, 2, 1);
    assertRuleAssociations(rulesWithEntities.get(1), ruleId1, 2, 1);

    // Verify rate limit config returned for specific entity
    Assertions.assertEquals(
        createdRateLimitRuleConfigOne, getRateLimitRuleConfigsForEntity(entityOne).get(0));
    // Verify no rate limit config for entity that isn't associated to any config
    RateLimitedEntity randomEntity =
        RateLimitedEntity.newBuilder()
            .setEntityId("random_entity")
            .setEntityType(RateLimitedEntityType.RATE_LIMITED_ENTITY_TYPE_API)
            .build();
    Assertions.assertTrue(getRateLimitRuleConfigsForEntity(randomEntity).isEmpty());

    // Delete association between entity3 and rule2
    DeleteRuleRateLimitedEntityAssociationRequest deleteRuleRateLimitedEntityAssociationRequest1 =
        DeleteRuleRateLimitedEntityAssociationRequest.newBuilder()
            .setEntity(entityThree)
            .setRuleId(ruleId2)
            .build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            rateLimitingConfigServiceStub.deleteRuleRateLimitedEntityAssociation(
                deleteRuleRateLimitedEntityAssociationRequest1));

    rulesWithEntities = getAllRateLimitingRule();
    // Assert association got deleted
    assertEquals(2, rulesWithEntities.size());
    assertRuleAssociations(rulesWithEntities.get(0), ruleId1, 2, 0);
    assertRuleAssociations(rulesWithEntities.get(1), ruleId1, 2, 0);

    // Update rule1 -> updatedRule1 and disable it too.
    RateLimitingRuleConfig updatedRuleConfig =
        RateLimitingRuleConfig.newBuilder()
            .setRuleId(ruleId1)
            .setRuleName("updatedRule1")
            .setDescription("this is rule 1")
            .setMaxCallCountAllowed(10L)
            .setMaxCallCountDurationMillis(100000L)
            .setRuleViolationAction(RuleViolationAction.RULE_VIOLATION_ACTION_SUSPEND)
            .setSuspendDurationMillis(1000L)
            .setDisabled(true)
            .build();

    UpdateRuleConfigRequest updateRuleConfigRequest =
        UpdateRuleConfigRequest.newBuilder().setRule(updatedRuleConfig).build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID, () -> rateLimitingConfigServiceStub.updateRuleConfig(updateRuleConfigRequest));

    // Assert rule update changed the fields
    rulesWithEntities = getAllRateLimitingRule();
    rulesWithEntities.stream()
        .filter(r -> r.getRule().getRuleId().equals(ruleId1))
        .forEach(
            r -> {
              assertEquals("updatedRule1", r.getRule().getRuleName());
              assertTrue(r.getRule().getDisabled());
            });

    // Delete rule1
    DeleteRuleConfigRequest deleteRuleConfigRequest =
        DeleteRuleConfigRequest.newBuilder().setRuleId(ruleId1).build();
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID, () -> rateLimitingConfigServiceStub.deleteRuleConfig(deleteRuleConfigRequest));
    // Assert rule1 got deleted
    rulesWithEntities = getAllRateLimitingRule();
    assertEquals(1, rulesWithEntities.size());
    assertEquals(ruleId2, rulesWithEntities.get(0).getRule().getRuleId());
  }

  private void assertRuleAssociations(
      RateLimitingRuleWithRateLimitedEntities ruleWithEntity,
      String ruleId1,
      long ruleId1ExpectedAssociationCount,
      long ruleId2ExpectedAssociationCount) {
    if (ruleWithEntity.getRule().getRuleId().equals(ruleId1)) {
      assertEquals(
          ruleId1ExpectedAssociationCount, ruleWithEntity.getEntitiesAssociatedList().size());
    } else {
      assertEquals(
          ruleId2ExpectedAssociationCount, ruleWithEntity.getEntitiesAssociatedList().size());
    }
  }

  private RateLimitingRuleConfig createRateLimitingRuleConfig(CreateRateLimitingRuleConfig config) {
    CreateRuleConfigRequest request = CreateRuleConfigRequest.newBuilder().setRule(config).build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> rateLimitingConfigServiceStub.createRuleConfig(request))
        .getRule();
  }

  private List<RateLimitingRuleWithRateLimitedEntities> getAllRateLimitingRule() {
    GetAllRateLimitingRulesRequest request = GetAllRateLimitingRulesRequest.newBuilder().build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID, () -> rateLimitingConfigServiceStub.getAllRateLimitingRules(request))
        .getRulesList();
  }

  private List<RateLimitingRuleConfig> getRateLimitRuleConfigsForEntity(
      RateLimitedEntity rateLimitedEntity) {
    GetRateLimitingConfigsForEntityRequest request =
        GetRateLimitingConfigsForEntityRequest.newBuilder().setEntity(rateLimitedEntity).build();
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () -> rateLimitingConfigServiceStub.getRateLimitingConfigsForEntity(request).getRuleList());
  }
}
