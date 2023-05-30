package ai.traceable.detection.exclusion.config.service.v1.rules;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import ai.traceable.detection.exclusion.config.service.v1.rules.migration.DetectionExclusionRulesMigrationManager;
import ai.traceable.detection.exclusion.config.service.v1.rules.migration.RulesMigrationManager;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DetectionExclusionRulesManagerTest {

  private UuidGenerator uuidGenerator;
  private DetectionExclusionRulesManager rulesManager;
  private final RequestContext requestContext = RequestContext.forTenantId("tenantId");

  @BeforeEach
  void setUp() {
    MockGenericConfigService mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    ConfigChangeEventGenerator mockConfigChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);
    DetectionExclusionConfigServiceConfig config = new DetectionExclusionConfigServiceConfig();
    DetectionExclusionRulesStore rulesStore =
        new DetectionExclusionRulesStore(
            configServiceBlockingStub, mockConfigChangeEventGenerator, config);
    uuidGenerator = mock(UuidGenerator.class);
    RulesMigrationManager rulesMigrationManager =
        mock(DetectionExclusionRulesMigrationManager.class);
    when(rulesMigrationManager.shouldMigrateFromOldStore(any())).thenReturn(false);
    rulesManager =
        new DetectionExclusionRulesManager(rulesStore, uuidGenerator, rulesMigrationManager);
  }

  @Test
  void testCRUDDetectionExclusionRules() {
    DetectionExclusionRuleInfo detectionExclusionRuleInfo =
        DetectionExclusionRuleInfo.newBuilder()
            .setName("rule")
            .setRuleStatus(DetectionExclusionRuleStatus.newBuilder().build())
            .build();
    DetectionExclusionRuleScope detectionExclusionRuleScope =
        DetectionExclusionRuleScope.getDefaultInstance();
    DetectionExclusionRule detectionExclusionRule =
        DetectionExclusionRule.newBuilder()
            .setId("id")
            .setRuleInfo(detectionExclusionRuleInfo)
            .setRuleScope(detectionExclusionRuleScope)
            .build();

    when(uuidGenerator.generateRandomId()).thenReturn("id");

    // creating detection exclusion rule
    assertEquals(
        detectionExclusionRule,
        rulesManager.createDetectionExclusionRule(
            requestContext, detectionExclusionRuleScope, detectionExclusionRuleInfo));

    // fetching detection exclusion rule
    assertTrue(
        rulesManager
            .getDetectionExclusionRules(requestContext, GetRulesFilter.getDefaultInstance())
            .contains(detectionExclusionRule));
    assertTrue(
        rulesManager
            .getDetectionExclusionRules(
                requestContext, GetRulesFilter.newBuilder().setDisabled(false).build())
            .contains(detectionExclusionRule));

    detectionExclusionRule =
        DetectionExclusionRule.newBuilder()
            .setId("id")
            .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("rule1"))
            .setRuleScope(detectionExclusionRuleScope)
            .build();

    // updating detection exclusion rule
    DetectionExclusionRuleInfo info =
        detectionExclusionRule.getRuleInfo().toBuilder()
            .setRuleStatus(DetectionExclusionRuleStatus.getDefaultInstance())
            .build();
    detectionExclusionRule = detectionExclusionRule.toBuilder().setRuleInfo(info).build();
    assertEquals(
        detectionExclusionRule,
        rulesManager.updateDetectionExclusionRule(requestContext, detectionExclusionRule));

    // deleting detection exclusion rule
    assertDoesNotThrow(() -> rulesManager.deleteDetectionExclusionRule(requestContext, "id"));

    // rule not present after deletion
    assertFalse(
        rulesManager
            .getDetectionExclusionRules(requestContext, GetRulesFilter.getDefaultInstance())
            .contains(detectionExclusionRule));
  }

  @Test
  void testMergedStatusOnUpdateDetectionExclusionRule() {
    DetectionExclusionRuleInfo detectionExclusionRuleInfo =
        DetectionExclusionRuleInfo.newBuilder()
            .setName("rule-1")
            .addAllConditions(List.of(DetectionExclusionCondition.getDefaultInstance()))
            .setRuleStatus(
                DetectionExclusionRuleStatus.newBuilder()
                    .setRuleCreationSource(RuleSource.RULE_SOURCE_CUSTOMER)
                    .setDisabled(false)
                    .setGenerateInternalEvents(true)
                    .build())
            .build();
    DetectionExclusionRuleScope detectionExclusionRuleScope =
        DetectionExclusionRuleScope.getDefaultInstance();
    when(uuidGenerator.generateRandomId()).thenReturn("id-1");
    rulesManager.createDetectionExclusionRule(
        requestContext, detectionExclusionRuleScope, detectionExclusionRuleInfo);
    DetectionExclusionRule rule =
        DetectionExclusionRule.newBuilder()
            .setId("id-1")
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("rule-2")
                    .setRuleStatus(
                        DetectionExclusionRuleStatus.newBuilder()
                            .setDisabled(true)
                            .setRuleCreationSource(RuleSource.RULE_SOURCE_UNSPECIFIED)
                            .setHidden(false)))
            .setRuleScope(detectionExclusionRuleScope)
            .build();

    DetectionExclusionRule updateRule =
        rulesManager.updateDetectionExclusionRule(requestContext, rule);
    DetectionExclusionRuleStatus updatedRuleStatus = updateRule.getRuleInfo().getRuleStatus();

    assertEquals("id-1", updateRule.getId());
    assertEquals("rule-2", updateRule.getRuleInfo().getName());
    assertEquals(RuleSource.RULE_SOURCE_CUSTOMER, updatedRuleStatus.getRuleCreationSource());
    assertTrue(updatedRuleStatus.getDisabled());
    assertFalse(updatedRuleStatus.getHidden());
    assertTrue(updatedRuleStatus.getGenerateInternalEvents());

    // enabling the rule again
    rule =
        DetectionExclusionRule.newBuilder()
            .setId("id-1")
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("rule-2")
                    .setRuleStatus(DetectionExclusionRuleStatus.newBuilder().setDisabled(false)))
            .setRuleScope(detectionExclusionRuleScope)
            .build();

    updateRule = rulesManager.updateDetectionExclusionRule(requestContext, rule);
    updatedRuleStatus = updateRule.getRuleInfo().getRuleStatus();
    assertFalse(updatedRuleStatus.getDisabled());
  }
}
