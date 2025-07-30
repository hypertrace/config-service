package ai.traceable.config.service;

import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_ALERT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.detection.exclusion.config.service.v1.CreateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DeleteDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceGrpc;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope;
import ai.traceable.detection.exclusion.config.service.v1.EventCondition;
import ai.traceable.detection.exclusion.config.service.v1.GetDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.IpReputationCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpReputationSeverity;
import ai.traceable.detection.exclusion.config.service.v1.RuleChangeSource;
import ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily;
import ai.traceable.detection.exclusion.config.service.v1.UpdateDetectionExclusionRuleRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DetectionExclusionConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static DetectionExclusionConfigServiceGrpc.DetectionExclusionConfigServiceBlockingStub
      detectionExclusionConfigServiceStub;
  private static final DetectionExclusionRuleInfo RULE_INFO1 =
      DetectionExclusionRuleInfo.newBuilder()
          .setName("ruleName1")
          .addExclusionTargets(EXCLUSION_TARGET_ALERT)
          .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
          .addConditions(
              DetectionExclusionCondition.newBuilder()
                  .setIpReputationCondition(
                      IpReputationCondition.newBuilder()
                          .setMaxIpReputationSeverity(
                              IpReputationSeverity.IP_REPUTATION_SEVERITY_MEDIUM)))
          .addConditions(
              DetectionExclusionCondition.newBuilder()
                  .setEventCondition(
                      EventCondition.newBuilder()
                          .addSystemDefinedEvents(
                              SystemDefinedEvent.newBuilder()
                                  .setEventFamily(
                                      SystemDefinedEventFamily
                                          .SYSTEM_DEFINED_EVENT_FAMILY_API_DEF))))
          .setRuleStatus(
              DetectionExclusionRuleStatus.newBuilder()
                  .setChangeSource(RuleChangeSource.RULE_CHANGE_SOURCE_CUSTOMER))
          .build();
  private static final DetectionExclusionRuleInfo RULE_INFO2 =
      DetectionExclusionRuleInfo.newBuilder()
          .setName("ruleName2")
          .addExclusionTargets(EXCLUSION_TARGET_ALERT)
          .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
          .addConditions(
              DetectionExclusionCondition.newBuilder()
                  .setEventCondition(
                      EventCondition.newBuilder()
                          .addSystemDefinedEvents(
                              SystemDefinedEvent.newBuilder()
                                  .setEventFamily(
                                      SystemDefinedEventFamily
                                          .SYSTEM_DEFINED_EVENT_FAMILY_MODSEC))))
          .setRuleStatus(
              DetectionExclusionRuleStatus.newBuilder()
                  .setChangeSource(RuleChangeSource.RULE_CHANGE_SOURCE_TRACEABLE))
          .build();
  private static final DetectionExclusionRuleScope RULE_SCOPE1 =
      DetectionExclusionRuleScope.newBuilder()
          .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env1").build())
          .build();
  private static final DetectionExclusionRuleScope RULE_SCOPE2 =
      DetectionExclusionRuleScope.newBuilder()
          .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env2").build())
          .build();
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenantId");

  @BeforeAll
  static void init() {
    detectionExclusionConfigServiceStub =
        DetectionExclusionConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testCRUDDetectionExclusionRules() {
    List<DetectionExclusionRule> rules = getRules(GetRulesFilter.getDefaultInstance());
    // default rules
    assertEquals(13, rules.size());

    // creating rule1
    CreateDetectionExclusionRuleRequest createRequest1 =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleInfo(RULE_INFO1)
            .setRuleScope(RULE_SCOPE1)
            .build();
    DetectionExclusionRule rule1 =
        REQUEST_CONTEXT.call(
            () ->
                detectionExclusionConfigServiceStub
                    .createDetectionExclusionRule(createRequest1)
                    .getRule());
    DetectionExclusionRule expectedRule =
        DetectionExclusionRule.newBuilder()
            .setId(rule1.getId())
            .setRuleInfo(RULE_INFO1)
            .setRuleScope(RULE_SCOPE1)
            .build();
    assertEquals(expectedRule, rule1);

    // creating rule2
    CreateDetectionExclusionRuleRequest createRequest2 =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleInfo(RULE_INFO2)
            .setRuleScope(RULE_SCOPE2)
            .build();
    DetectionExclusionRule rule2 =
        REQUEST_CONTEXT.call(
            () ->
                detectionExclusionConfigServiceStub
                    .createDetectionExclusionRule(createRequest2)
                    .getRule());
    expectedRule =
        DetectionExclusionRule.newBuilder()
            .setId(rule2.getId())
            .setRuleInfo(RULE_INFO2)
            .setRuleScope(RULE_SCOPE2)
            .build();
    assertEquals(expectedRule, rule2);

    // fetching rules with and without filter
    rules = getRules(GetRulesFilter.getDefaultInstance());
    assertTrue(rules.contains(rule1));
    assertTrue(rules.contains(rule2));

    rules = getRules(GetRulesFilter.newBuilder().setRuleScope(RULE_SCOPE1).build());
    assertTrue(rules.contains(rule1));
    assertFalse(rules.contains(rule2));

    rules = getRules(GetRulesFilter.newBuilder().setRuleScope(RULE_SCOPE2).build());
    assertFalse(rules.contains(rule1));
    assertTrue(rules.contains(rule2));

    // updating rule1
    UpdateDetectionExclusionRuleRequest updateRequest =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(rule1.toBuilder().setRuleScope(RULE_SCOPE2))
            .build();
    rule1 =
        REQUEST_CONTEXT.call(
            () ->
                detectionExclusionConfigServiceStub
                    .updateDetectionExclusionRule(updateRequest)
                    .getRule());
    expectedRule =
        DetectionExclusionRule.newBuilder()
            .setId(rule1.getId())
            .setRuleInfo(RULE_INFO1)
            .setRuleScope(RULE_SCOPE2)
            .build();
    assertEquals(expectedRule, rule1);

    // deleting rule1
    DeleteDetectionExclusionRuleRequest deleteRequest =
        DeleteDetectionExclusionRuleRequest.newBuilder().setId(rule1.getId()).build();
    REQUEST_CONTEXT.call(
        () -> detectionExclusionConfigServiceStub.deleteDetectionExclusionRule(deleteRequest));
    rules = getRules(GetRulesFilter.getDefaultInstance());
    assertFalse(rules.contains(rule1));
  }

  private List<DetectionExclusionRule> getRules(GetRulesFilter rulesFilter) {
    return REQUEST_CONTEXT.call(
        () ->
            detectionExclusionConfigServiceStub
                .getDetectionExclusionRules(
                    GetDetectionExclusionRulesRequest.newBuilder().setFilter(rulesFilter).build())
                .getRulesList());
  }
}
