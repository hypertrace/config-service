package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.iprange.config.service.v1.*;
import java.util.Arrays;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class IpRangeConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {
  private static IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub ipRangeConfigServiceStub;

  @BeforeAll
  static void init() {
    ipRangeConfigServiceStub =
        IpRangeConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
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
                            .build())
                    .getRule()
                    .getId());

    IpRangeRule ipRangeRule1 =
        IpRangeRule.newBuilder()
            .setId(ruleId1)
            .setRuleDetails(ipRangeRuleDetails1)
            .addAllIpAddresses(Arrays.asList("1.2.3.4"))
            .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
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
                            .build())
                    .getRule()
                    .getId());

    IpRangeRule ipRangeRule2 =
        IpRangeRule.newBuilder()
            .setId(ruleId2)
            .setRuleDetails(ipRangeRuleDetails2)
            .addAllIpRanges(Arrays.asList("16.16.16.16/16"))
            .addAllIpAddresses(Arrays.asList("11.12.13.14"))
            .build();

    List<IpRangeRule> ipRangeRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                ipRangeConfigServiceStub
                    .getIpRangeRules(GetIpRangeRulesRequest.getDefaultInstance())
                    .getRulesList());

    assertEquals(List.of(ipRangeRule2, ipRangeRule1), ipRangeRules);
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
                            .build())
                    .getRule());
    IpRangeRule ipRangeRule1 =
        IpRangeRule.newBuilder()
            .setId(createdIpRangeRule1.getId())
            .setRuleDetails(ipRangeRuleDetails1)
            .addAllIpAddresses(Arrays.asList("1.2.3.4"))
            .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
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
                            .build())
                    .getRule());

    IpRangeRule ipRangeRule2 =
        IpRangeRule.newBuilder()
            .setId(createdIpRangeRule2.getId())
            .setRuleDetails(ipRangeRuleDetails2)
            .addAllIpRanges(Arrays.asList("16.16.16.16/16"))
            .addAllIpAddresses(Arrays.asList("11.12.13.14"))
            .build();

    assertEquals(ipRangeRule1, createdIpRangeRule1);
    assertEquals(ipRangeRule2, createdIpRangeRule2);
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
                ExpirationDetails.newBuilder().setExpirationDuration("PT1H2M34S").build())
            .build();

    String ruleId =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                ipRangeConfigServiceStub
                    .createIpRangeRule(
                        CreateIpRangeRuleRequest.newBuilder()
                            .setRuleDetails(ipRangeRuleDetails)
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
                            .build())
                    .getRule());

    assertEquals(updatedIpRangeRule, returnedIpRangeRule);
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
                            .build())
                    .getRule()
                    .getId());

    IpRangeRule ipRangeRule1 =
        IpRangeRule.newBuilder()
            .setId(ruleId1)
            .setRuleDetails(ipRangeRuleDetails1)
            .addAllIpAddresses(Arrays.asList("1.2.3.4"))
            .addAllIpRanges(Arrays.asList("1.1.1.1/16"))
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

    assertEquals(List.of(ipRangeRule1), ipRangeRules);
  }
}
