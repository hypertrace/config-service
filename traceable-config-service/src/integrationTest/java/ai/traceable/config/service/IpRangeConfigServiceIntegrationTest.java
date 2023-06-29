package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.iprange.config.service.v1.CreateIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.DeleteIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.EnvironmentScope;
import ai.traceable.iprange.config.service.v1.ExpirationDetails;
import ai.traceable.iprange.config.service.v1.GetIpRangeRulesRequest;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.iprange.config.service.v1.IpRangeRuleDetails;
import ai.traceable.iprange.config.service.v1.RuleAction;
import ai.traceable.iprange.config.service.v1.RuleScope;
import ai.traceable.iprange.config.service.v1.UpdateIpRangeRuleRequest;
import java.util.Arrays;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class IpRangeConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {
  private static IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub ipRangeConfigServiceStub;
  private static final RuleScope ruleScope1 =
      RuleScope.newBuilder()
          .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env1").build())
          .build();
  private static final RuleScope ruleScope2 =
      RuleScope.newBuilder()
          .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env2").build())
          .build();

  @BeforeAll
  static void init() {
    ipRangeConfigServiceStub =
        IpRangeConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  public void getIpRangeRules() {
    IpRangeRuleDetails ipRangeRuleDetails1 =
        IpRangeRuleDetails.newBuilder()
            .setName("Tester-1")
            .setDescription("Range rule test 1")
            .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
            .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
            .setExpirationDetails(
                ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
            .build();

    String ruleId1 =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                ipRangeConfigServiceStub
                    .createIpRangeRule(
                        CreateIpRangeRuleRequest.newBuilder()
                            .setRuleDetails(ipRangeRuleDetails1)
                            .setRuleScope(ruleScope1)
                            .build())
                    .getRule()
                    .getId());

    IpRangeRule ipRangeRule1 =
        IpRangeRule.newBuilder()
            .setId(ruleId1)
            .setRuleDetails(ipRangeRuleDetails1)
            .addAllIpAddresses(Arrays.asList("1.2.3.4"))
            .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
            .setRuleScope(ruleScope1)
            .build();

    IpRangeRuleDetails ipRangeRuleDetails2 =
        IpRangeRuleDetails.newBuilder()
            .setName("Tester-2")
            .setDescription("Range rule test 2")
            .addAllRawInputIpData(Arrays.asList("11.12.13.14", "16.16.16.16/16"))
            .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
            .setExpirationDetails(
                ExpirationDetails.newBuilder().setExpirationTimestampMillis(1000).build())
            .build();

    String ruleId2 =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                ipRangeConfigServiceStub
                    .createIpRangeRule(
                        CreateIpRangeRuleRequest.newBuilder()
                            .setRuleDetails(ipRangeRuleDetails2)
                            .setRuleScope(ruleScope2)
                            .build())
                    .getRule()
                    .getId());

    IpRangeRule ipRangeRule2 =
        IpRangeRule.newBuilder()
            .setId(ruleId2)
            .setRuleDetails(ipRangeRuleDetails2)
            .addAllIpRanges(Arrays.asList("16.16.16.16/16"))
            .addAllIpAddresses(Arrays.asList("11.12.13.14"))
            .setRuleScope(ruleScope2)
            .build();

    List<IpRangeRule> ipRangeRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                ipRangeConfigServiceStub
                    .getIpRangeRules(GetIpRangeRulesRequest.getDefaultInstance())
                    .getRulesList());

    assertCustom(List.of(ipRangeRule2, ipRangeRule1), ipRangeRules);
  }

  @Test
  public void createIpRangeRule() {
    IpRangeRuleDetails ipRangeRuleDetails1 =
        IpRangeRuleDetails.newBuilder()
            .setName("Tester-1")
            .setDescription("Range rule test 1")
            .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
            .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
            .setExpirationDetails(
                ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
            .build();

    IpRangeRule createdIpRangeRule1 =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                ipRangeConfigServiceStub
                    .createIpRangeRule(
                        CreateIpRangeRuleRequest.newBuilder()
                            .setRuleDetails(ipRangeRuleDetails1)
                            .setRuleScope(ruleScope1)
                            .build())
                    .getRule());
    IpRangeRule ipRangeRule1 =
        IpRangeRule.newBuilder()
            .setId(createdIpRangeRule1.getId())
            .setRuleDetails(ipRangeRuleDetails1)
            .addAllIpAddresses(Arrays.asList("1.2.3.4"))
            .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
            .setRuleScope(ruleScope1)
            .build();

    IpRangeRuleDetails ipRangeRuleDetails2 =
        IpRangeRuleDetails.newBuilder()
            .setName("Tester-2")
            .setDescription("Range rule test 2")
            .addAllRawInputIpData(Arrays.asList("11.12.13.14", "16.16.16.16/16"))
            .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
            .setExpirationDetails(
                ExpirationDetails.newBuilder().setExpirationTimestampMillis(1000).build())
            .build();

    IpRangeRule createdIpRangeRule2 =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                ipRangeConfigServiceStub
                    .createIpRangeRule(
                        CreateIpRangeRuleRequest.newBuilder()
                            .setRuleDetails(ipRangeRuleDetails2)
                            .setRuleScope(ruleScope2)
                            .build())
                    .getRule());

    IpRangeRule ipRangeRule2 =
        IpRangeRule.newBuilder()
            .setId(createdIpRangeRule2.getId())
            .setRuleDetails(ipRangeRuleDetails2)
            .addAllIpRanges(Arrays.asList("16.16.16.16/16"))
            .addAllIpAddresses(Arrays.asList("11.12.13.14"))
            .setRuleScope(ruleScope2)
            .build();

    assertCustom(ipRangeRule1, createdIpRangeRule1);
    assertCustom(ipRangeRule2, createdIpRangeRule2);
  }

  @Test
  public void updateIpRangeRule() {
    IpRangeRuleDetails ipRangeRuleDetails =
        IpRangeRuleDetails.newBuilder()
            .setName("Tester-1")
            .setDescription("Range rule test 1")
            .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
            .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
            .setExpirationDetails(
                ExpirationDetails.newBuilder().setExpirationTimestampMillis(1623226263462L).build())
            .build();

    String ruleId =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                ipRangeConfigServiceStub
                    .createIpRangeRule(
                        CreateIpRangeRuleRequest.newBuilder()
                            .setRuleDetails(ipRangeRuleDetails)
                            .setRuleScope(ruleScope1)
                            .build())
                    .getRule()
                    .getId());

    IpRangeRuleDetails updatedIpRangeRuleDetails =
        IpRangeRuleDetails.newBuilder()
            .setName("Updated-Tester-1")
            .setDescription("Updated Range rule test 1")
            .addAllRawInputIpData(Arrays.asList("11.12.13.14", "1.1.1.1/16"))
            .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
            .setExpirationDetails(
                ExpirationDetails.newBuilder().setExpirationDuration("PT1H3M34S").build())
            .build();

    IpRangeRule updatedIpRangeRule =
        IpRangeRule.newBuilder()
            .setId(ruleId)
            .setRuleDetails(updatedIpRangeRuleDetails)
            .addAllIpAddresses(Arrays.asList("11.12.13.14"))
            .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
            .setDisabled(true)
            .setInternal(true)
            .setRuleScope(ruleScope2)
            .build();

    IpRangeRule returnedIpRangeRule =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                ipRangeConfigServiceStub
                    .updateIpRangeRule(
                        UpdateIpRangeRuleRequest.newBuilder()
                            .setId(ruleId)
                            .setRuleDetails(updatedIpRangeRuleDetails)
                            .setInternal(true)
                            .setDisabled(true)
                            .setRuleScope(ruleScope2)
                            .build())
                    .getRule());

    assertCustom(updatedIpRangeRule, returnedIpRangeRule);
    assertEquals(
        1,
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    ipRangeConfigServiceStub
                        .getIpRangeRules(GetIpRangeRulesRequest.getDefaultInstance())
                        .getRulesList())
            .size());
  }

  @Test
  public void deleteIpRangeRule() {
    IpRangeRuleDetails ipRangeRuleDetails1 =
        IpRangeRuleDetails.newBuilder()
            .setName("Tester-1")
            .setDescription("Range rule test 1")
            .addAllRawInputIpData(Arrays.asList("1.2.3.4", "1.1.1.1/16"))
            .setRuleAction(RuleAction.RULE_ACTION_BLOCK)
            .setExpirationDetails(
                ExpirationDetails.newBuilder().setExpirationTimestampMillis(1623226263462L).build())
            .build();

    String ruleId1 =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                ipRangeConfigServiceStub
                    .createIpRangeRule(
                        CreateIpRangeRuleRequest.newBuilder()
                            .setRuleDetails(ipRangeRuleDetails1)
                            .setRuleScope(ruleScope1)
                            .build())
                    .getRule()
                    .getId());

    IpRangeRule ipRangeRule1 =
        IpRangeRule.newBuilder()
            .setId(ruleId1)
            .setRuleDetails(ipRangeRuleDetails1)
            .addAllIpAddresses(Arrays.asList("1.2.3.4"))
            .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
            .setRuleScope(ruleScope1)
            .build();

    IpRangeRuleDetails ipRangeRuleDetails2 =
        IpRangeRuleDetails.newBuilder()
            .setName("Tester-2")
            .setDescription("Range rule test 2")
            .addAllRawInputIpData(Arrays.asList("11.12.13.14", "16.16.16.16/16"))
            .setRuleAction(RuleAction.RULE_ACTION_ALLOW)
            .setExpirationDetails(
                ExpirationDetails.newBuilder().setExpirationTimestampMillis(1000).build())
            .build();

    String ruleId2 =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                ipRangeConfigServiceStub
                    .createIpRangeRule(
                        CreateIpRangeRuleRequest.newBuilder()
                            .setRuleDetails(ipRangeRuleDetails2)
                            .setRuleScope(ruleScope2)
                            .build())
                    .getRule()
                    .getId());

    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            ipRangeConfigServiceStub.deleteIpRangeRule(
                DeleteIpRangeRuleRequest.newBuilder().setId(ruleId2).build()));
    List<IpRangeRule> ipRangeRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                ipRangeConfigServiceStub
                    .getIpRangeRules(GetIpRangeRulesRequest.getDefaultInstance())
                    .getRulesList());

    assertCustom(List.of(ipRangeRule1), ipRangeRules);
  }

  private boolean assertCustom(IpRangeRule ipRangeRule1, IpRangeRule ipRangeRule2) {
    assertEquals(ipRangeRule1.getId(), ipRangeRule2.getId());
    assertEquals(ipRangeRule1.getRuleDetails().getName(), ipRangeRule2.getRuleDetails().getName());
    assertEquals(
        ipRangeRule1.getRuleDetails().getDescription(),
        ipRangeRule2.getRuleDetails().getDescription());
    assertEquals(
        ipRangeRule1.getRuleDetails().getRawInputIpDataList(),
        ipRangeRule2.getRuleDetails().getRawInputIpDataList());
    assertEquals(
        ipRangeRule1.getRuleDetails().getRuleAction(),
        ipRangeRule2.getRuleDetails().getRuleAction());
    assertEquals(
        ipRangeRule1.getRuleScope().getEnvironmentScope().getEnvironmentIdsList(),
        ipRangeRule2.getRuleScope().getEnvironmentScope().getEnvironmentIdsList());
    if (ipRangeRule1.getRuleDetails().hasExpirationDetails()
        && ipRangeRule2.getRuleDetails().hasExpirationDetails()) {
      assertTrue(
          ipRangeRule1
                  .getRuleDetails()
                  .getExpirationDetails()
                  .getExpirationDuration()
                  .equals(
                      ipRangeRule2.getRuleDetails().getExpirationDetails().getExpirationDuration())
              || ipRangeRule1.getRuleDetails().getExpirationDetails().getExpirationTimestampMillis()
                  == ipRangeRule2
                      .getRuleDetails()
                      .getExpirationDetails()
                      .getExpirationTimestampMillis());
    }
    assertEquals(ipRangeRule1.getDisabled(), ipRangeRule2.getDisabled());
    assertEquals(ipRangeRule1.getInternal(), ipRangeRule2.getInternal());
    assertEquals(ipRangeRule1.getIpRangesList(), ipRangeRule2.getIpRangesList());
    assertEquals(ipRangeRule1.getIpAddressesList(), ipRangeRule2.getIpAddressesList());
    return true;
  }

  private boolean assertCustom(List<IpRangeRule> ipRangeRules1, List<IpRangeRule> ipRangeRules2) {
    assertEquals(ipRangeRules1.size(), ipRangeRules2.size());
    for (int i = 0; i < ipRangeRules1.size(); i++) {
      assertCustom(ipRangeRules1.get(i), ipRangeRules2.get(i));
    }
    return true;
  }
}
