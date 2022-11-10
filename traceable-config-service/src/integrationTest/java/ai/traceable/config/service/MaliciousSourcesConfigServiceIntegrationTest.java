package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.DeleteMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.EnvironmentScope;
import ai.traceable.malicioussources.config.service.v1.EventSeverity;
import ai.traceable.malicioussources.config.service.v1.GetMaliciousSourcesRulesRequest;
import ai.traceable.malicioussources.config.service.v1.GetRulesFilter;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.IpLocationTypeCondition;
import ai.traceable.malicioussources.config.service.v1.IpReputationCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleAction;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleScope;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleStatus;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import ai.traceable.malicioussources.config.service.v1.UpdateMaliciousSourcesRuleRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MaliciousSourcesConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub
      maliciousSourcesConfigServiceBlockingStub;
  private static final MaliciousSourcesRuleScope ruleScope1 =
      MaliciousSourcesRuleScope.newBuilder()
          .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env1").build())
          .build();
  private static final MaliciousSourcesRuleScope ruleScope2 =
      MaliciousSourcesRuleScope.newBuilder()
          .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env2").build())
          .build();

  private static final MaliciousSourcesRuleInfo maliciousSourcesRuleInfo1 =
      MaliciousSourcesRuleInfo.newBuilder()
          .setName("Tester-1")
          .setDescription("Malicious Sources Rule Test")
          .setRuleAction(
              MaliciousSourcesRuleAction.newBuilder()
                  .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK)
                  .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL))
          .addConditions(
              MaliciousSourcesRuleCondition.newBuilder()
                  .setIpReputationCondition(
                      IpReputationCondition.newBuilder().setMinIpReputationScore(100)))
          .build();
  private static final MaliciousSourcesRuleInfo maliciousSourcesRuleInfo2 =
      MaliciousSourcesRuleInfo.newBuilder()
          .setName("Tester-2")
          .setDescription("Malicious Sources Rule Test")
          .setRuleAction(
              MaliciousSourcesRuleAction.newBuilder()
                  .setActionType(RuleActionType.RULE_ACTION_TYPE_ALERT)
                  .setEventSeverity(EventSeverity.EVENT_SEVERITY_CRITICAL))
          .addConditions(
              MaliciousSourcesRuleCondition.newBuilder()
                  .setIpLocationTypeCondition(
                      IpLocationTypeCondition.newBuilder()
                          .addIpLocationTypes(IpLocationType.IP_LOCATION_TYPE_ANONYMOUS_VPN)))
          .build();

  @BeforeAll
  static void init() {
    maliciousSourcesConfigServiceBlockingStub =
        MaliciousSourcesConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  public void getMaliciousSourcesRules() {
    CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest1 =
        CreateMaliciousSourcesRuleRequest.newBuilder()
            .setRuleInfo(maliciousSourcesRuleInfo1)
            .setRuleScope(ruleScope1)
            .build();

    String ruleId1 =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    maliciousSourcesConfigServiceBlockingStub
                        .createMaliciousSourcesRule(createMaliciousSourcesRuleRequest1)
                        .getRule()
                        .getId());

    MaliciousSourcesRule expectedMaliciousSourcesRule1 =
        getMaliciousSourcesRule(ruleId1, maliciousSourcesRuleInfo1, ruleScope1);

    CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest2 =
        CreateMaliciousSourcesRuleRequest.newBuilder()
            .setRuleInfo(maliciousSourcesRuleInfo2)
            .setRuleScope(ruleScope2)
            .build();

    RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                maliciousSourcesConfigServiceBlockingStub.createMaliciousSourcesRule(
                    createMaliciousSourcesRuleRequest2));

    List<MaliciousSourcesRule> actualMaliciousSourcesRules =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    maliciousSourcesConfigServiceBlockingStub
                        .getMaliciousSourcesRules(
                            GetMaliciousSourcesRulesRequest.newBuilder()
                                .setFilter(
                                    GetRulesFilter.newBuilder().setRuleScope(ruleScope1).build())
                                .build())
                        .getRulesList());
    assertEquals(List.of(expectedMaliciousSourcesRule1), actualMaliciousSourcesRules);
  }

  @Test
  public void createMaliciousSourceRangeRule() {
    CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest1 =
        CreateMaliciousSourcesRuleRequest.newBuilder()
            .setRuleInfo(maliciousSourcesRuleInfo1)
            .setRuleScope(ruleScope1)
            .build();
    MaliciousSourcesRule createdMaliciousSourcesRule1 =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    maliciousSourcesConfigServiceBlockingStub
                        .createMaliciousSourcesRule(createMaliciousSourcesRuleRequest1)
                        .getRule());
    MaliciousSourcesRule expectedMaliciousSourcesRule1 =
        getMaliciousSourcesRule(
            createdMaliciousSourcesRule1.getId(), maliciousSourcesRuleInfo1, ruleScope1);

    CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest2 =
        CreateMaliciousSourcesRuleRequest.newBuilder()
            .setRuleInfo(maliciousSourcesRuleInfo2)
            .setRuleScope(ruleScope2)
            .build();

    MaliciousSourcesRule createdMaliciousSourcesRule2 =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    maliciousSourcesConfigServiceBlockingStub
                        .createMaliciousSourcesRule(createMaliciousSourcesRuleRequest2)
                        .getRule());
    MaliciousSourcesRule expectedMaliciousSourcesRule2 =
        getMaliciousSourcesRule(
            createdMaliciousSourcesRule2.getId(), maliciousSourcesRuleInfo2, ruleScope2);

    assertEquals(expectedMaliciousSourcesRule1, createdMaliciousSourcesRule1);
    assertEquals(expectedMaliciousSourcesRule2, createdMaliciousSourcesRule2);
  }

  @Test
  public void updateMaliciousSourcesRangeRule() {
    CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest =
        CreateMaliciousSourcesRuleRequest.newBuilder()
            .setRuleInfo(maliciousSourcesRuleInfo1)
            .setRuleScope(ruleScope1)
            .build();
    String ruleId =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    maliciousSourcesConfigServiceBlockingStub
                        .createMaliciousSourcesRule(createMaliciousSourcesRuleRequest)
                        .getRule()
                        .getId());

    MaliciousSourcesRule updatedMaliciousSourcesRule =
        MaliciousSourcesRule.newBuilder()
            .setId(ruleId)
            .setRuleInfo(maliciousSourcesRuleInfo2)
            .setRuleScope(ruleScope2)
            .setRuleStatus(
                MaliciousSourcesRuleStatus.newBuilder().setDisabled(true).setInternal(true).build())
            .build();
    UpdateMaliciousSourcesRuleRequest updateMaliciousSourcesRuleRequest =
        UpdateMaliciousSourcesRuleRequest.newBuilder().setRule(updatedMaliciousSourcesRule).build();
    MaliciousSourcesRule returnedMaliciousSourcesRule =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    maliciousSourcesConfigServiceBlockingStub
                        .updateMaliciousSourcesRule(updateMaliciousSourcesRuleRequest)
                        .getRule());

    assertEquals(updatedMaliciousSourcesRule, returnedMaliciousSourcesRule);
    assertEquals(
        1,
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    maliciousSourcesConfigServiceBlockingStub
                        .getMaliciousSourcesRules(
                            GetMaliciousSourcesRulesRequest.getDefaultInstance())
                        .getRulesList()
                        .size()));
  }

  @Test
  public void deleteMaliciousSourcesRule() {
    CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest1 =
        CreateMaliciousSourcesRuleRequest.newBuilder()
            .setRuleInfo(maliciousSourcesRuleInfo1)
            .setRuleScope(ruleScope1)
            .build();
    String ruleId1 =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    maliciousSourcesConfigServiceBlockingStub
                        .createMaliciousSourcesRule(createMaliciousSourcesRuleRequest1)
                        .getRule()
                        .getId());

    MaliciousSourcesRule expectedMaliciousSourcesRule1 =
        getMaliciousSourcesRule(ruleId1, maliciousSourcesRuleInfo1, ruleScope1);

    CreateMaliciousSourcesRuleRequest createMaliciousSourcesRuleRequest2 =
        CreateMaliciousSourcesRuleRequest.newBuilder()
            .setRuleInfo(maliciousSourcesRuleInfo2)
            .setRuleScope(ruleScope2)
            .build();

    String ruleId2 =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    maliciousSourcesConfigServiceBlockingStub
                        .createMaliciousSourcesRule(createMaliciousSourcesRuleRequest2)
                        .getRule()
                        .getId());
    RequestContext.forTenantId(TENANT_ID)
        .call(
            () ->
                maliciousSourcesConfigServiceBlockingStub.deleteMaliciousSourcesRule(
                    DeleteMaliciousSourcesRuleRequest.newBuilder().setId(ruleId2).buildPartial()));
    List<MaliciousSourcesRule> actualMaliciousSourcesRules =
        RequestContext.forTenantId(TENANT_ID)
            .call(
                () ->
                    maliciousSourcesConfigServiceBlockingStub
                        .getMaliciousSourcesRules(
                            GetMaliciousSourcesRulesRequest.getDefaultInstance())
                        .getRulesList());

    assertEquals(List.of(expectedMaliciousSourcesRule1), actualMaliciousSourcesRules);
  }

  private MaliciousSourcesRule getMaliciousSourcesRule(
      String ruleId,
      MaliciousSourcesRuleInfo maliciousSourcesRuleInfo,
      MaliciousSourcesRuleScope ruleScope) {
    return MaliciousSourcesRule.newBuilder()
        .setId(ruleId)
        .setRuleInfo(maliciousSourcesRuleInfo)
        .setRuleScope(ruleScope)
        .build();
  }
}
