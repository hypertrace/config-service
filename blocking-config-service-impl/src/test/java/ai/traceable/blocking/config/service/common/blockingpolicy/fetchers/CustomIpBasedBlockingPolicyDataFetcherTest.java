package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_ALERT;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_ALLOW;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_BLOCK;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.v2.AttributeScope;
import ai.traceable.blocking.config.service.v2.InlineModification;
import ai.traceable.blocking.config.service.v2.RuleAction;
import ai.traceable.iprange.config.service.v1.AgentModification;
import ai.traceable.iprange.config.service.v1.AgentRuleEffect;
import ai.traceable.iprange.config.service.v1.EnvironmentScope;
import ai.traceable.iprange.config.service.v1.ExpirationDetails;
import ai.traceable.iprange.config.service.v1.FieldValue;
import ai.traceable.iprange.config.service.v1.GetIpRangeRulesRequest;
import ai.traceable.iprange.config.service.v1.GetIpRangeRulesResponse;
import ai.traceable.iprange.config.service.v1.GetRulesFilter;
import ai.traceable.iprange.config.service.v1.HeaderInjection;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.iprange.config.service.v1.IpRangeRuleDetails;
import ai.traceable.iprange.config.service.v1.PredicateLocation;
import ai.traceable.iprange.config.service.v1.RuleEffectWithModifications;
import ai.traceable.iprange.config.service.v1.RuleScope;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

class CustomIpBasedBlockingPolicyDataFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final GetIpRangeRulesRequest DEFAULT_GET_REQUEST =
      GetIpRangeRulesRequest.newBuilder()
          .setFilter(
              GetRulesFilter.newBuilder()
                  .setDisabled(false)
                  .setRuleScope(
                      RuleScope.newBuilder().setEnvironmentScope(EnvironmentScope.newBuilder())))
          .build();

  private IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub ipRangeConfigServiceStub;
  private CustomIpBasedBlockingPolicyDataFetcher customIpBasedDataFetcher;
  private static final long inactiveTimestamp = System.currentTimeMillis() - 10000L;
  private static final long activeTimestamp = System.currentTimeMillis() + 10000L;

  @BeforeEach
  void setUp() {
    ipRangeConfigServiceStub =
        mock(IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub.class, Answers.RETURNS_SELF);

    BlockingRulesUtils blockingRulesUtils = mock(BlockingRulesUtils.class);
    doReturn(true).when(blockingRulesUtils).isRuleActive(activeTimestamp);
    doReturn(true).when(blockingRulesUtils).isRuleActive(0);
    doReturn(false).when(blockingRulesUtils).isRuleActive(inactiveTimestamp);

    when(blockingRulesUtils.generateBlockingStatus(0, BlockingPolicyData.RuleType.ALLOW))
        .thenReturn(BlockingPolicyData.Status.ALLOWED);
    when(blockingRulesUtils.generateBlockingStatus(0, BlockingPolicyData.RuleType.BLOCK))
        .thenReturn(BlockingPolicyData.Status.DENIED);
    when(blockingRulesUtils.generateBlockingStatus(0, BlockingPolicyData.RuleType.BLOCK_ALL_EXCEPT))
        .thenReturn(BlockingPolicyData.Status.DENIED);

    when(blockingRulesUtils.generateBlockingStatus(
            activeTimestamp, BlockingPolicyData.RuleType.ALLOW))
        .thenReturn(BlockingPolicyData.Status.SNOOZED);
    when(blockingRulesUtils.generateBlockingStatus(
            activeTimestamp, BlockingPolicyData.RuleType.BLOCK))
        .thenReturn(BlockingPolicyData.Status.SUSPENDED);
    when(blockingRulesUtils.generateBlockingStatus(
            activeTimestamp, BlockingPolicyData.RuleType.BLOCK_ALL_EXCEPT))
        .thenReturn(BlockingPolicyData.Status.SUSPENDED);

    customIpBasedDataFetcher =
        new CustomIpBasedBlockingPolicyDataFetcher(
            ipRangeConfigServiceStub, blockingRulesUtils, ClientConfig.DEFAULT);
  }

  @Test
  void getCustomIpBasedRulesTestEmpty() {
    doReturn(GetIpRangeRulesResponse.getDefaultInstance())
        .when(ipRangeConfigServiceStub)
        .getIpRangeRules(DEFAULT_GET_REQUEST);

    List<BlockingPolicyData> customIpBasedRuleList =
        customIpBasedDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList();
    assertEquals(0, customIpBasedRuleList.size());
  }

  @Test
  void getCustomIpBasedRulesTestWithoutEnvironment() {
    doReturn(sampleGetIpRangeRulesAllEnvsResponse)
        .when(ipRangeConfigServiceStub)
        .getIpRangeRules(DEFAULT_GET_REQUEST);

    List<BlockingPolicyData> customIpBasedRuleList =
        customIpBasedDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList();

    assertEquals(5, customIpBasedRuleList.size());
    assertEquals(
        IpBlockingDetails.builder().ipAddresses(List.of("1.2.3.4", "11.22.33.44")).build(),
        customIpBasedRuleList.get(0).getBlockingDetails());
    assertEquals(
        IpBlockingDetails.builder().ipRanges(List.of("1.2.3.4")).build(),
        customIpBasedRuleList.get(1).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_IP_RULE, customIpBasedRuleList.get(0).getCategory());
    assertEquals(BlockingPolicyData.RuleType.BLOCK, customIpBasedRuleList.get(0).getRuleType());
    assertEquals(BlockingPolicyData.Status.SUSPENDED, customIpBasedRuleList.get(0).getStatus());
    assertEquals(activeTimestamp, customIpBasedRuleList.get(0).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomIpRuleViolationInfo("rule-id-1", "rule-name-1"),
        customIpBasedRuleList.get(0).getInfo());
    assertEquals("rule-id-1", customIpBasedRuleList.get(0).getRuleId());

    assertEquals(
        IpBlockingDetails.builder().ipAddresses(List.of("1.2.3.4", "11.22.33.44")).build(),
        customIpBasedRuleList.get(2).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_IP_RULE, customIpBasedRuleList.get(2).getCategory());
    assertEquals(BlockingPolicyData.RuleType.ALLOW, customIpBasedRuleList.get(2).getRuleType());
    assertEquals(BlockingPolicyData.Status.SNOOZED, customIpBasedRuleList.get(2).getStatus());
    assertEquals(activeTimestamp, customIpBasedRuleList.get(2).getTimestamp());
    assertEquals(
        ExemptionInfoEncoder.getEncodedCustomIpRuleExemptionInfo("rule-id-5", "rule-name-5"),
        customIpBasedRuleList.get(2).getInfo());
    assertEquals("rule-id-5", customIpBasedRuleList.get(2).getRuleId());

    assertEquals(
        IpBlockingDetails.builder().ipAddresses(List.of("1.2.3.4", "11.22.33.44")).build(),
        customIpBasedRuleList.get(3).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_IP_RULE, customIpBasedRuleList.get(3).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.BLOCK_ALL_EXCEPT, customIpBasedRuleList.get(3).getRuleType());
    assertEquals(BlockingPolicyData.Status.SUSPENDED, customIpBasedRuleList.get(3).getStatus());
    assertEquals(activeTimestamp, customIpBasedRuleList.get(3).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomIpRuleViolationInfo("rule-id-7", "rule-name-7"),
        customIpBasedRuleList.get(3).getInfo());
    assertEquals("rule-id-7", customIpBasedRuleList.get(3).getRuleId());

    assertEquals(
        IpBlockingDetails.builder().ipRange("5.6.7.8").build(),
        customIpBasedRuleList.get(4).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_IP_RULE, customIpBasedRuleList.get(4).getCategory());
    assertEquals(BlockingPolicyData.RuleType.ANALYTICS, customIpBasedRuleList.get(4).getRuleType());
    assertNull(customIpBasedRuleList.get(4).getStatus());
    assertEquals(activeTimestamp, customIpBasedRuleList.get(4).getTimestamp());
    assertEquals("Rule has some non-blocking agent action", customIpBasedRuleList.get(4).getInfo());
    assertEquals("rule-id-8", customIpBasedRuleList.get(4).getRuleId());
    RuleAction expectedRuleAction =
        RuleAction.newBuilder()
            .addInlineModifications(
                InlineModification.newBuilder()
                    .setHeaderInjection(
                        ai.traceable.blocking.config.service.v2.HeaderInjection.newBuilder()
                            .setScope(AttributeScope.ATTRIBUTE_SCOPE_REQUEST)
                            .setHeaderName("test-header")
                            .setValue(
                                ai.traceable.blocking.config.service.v2.FieldValue.newBuilder()
                                    .setStaticValue("test-value"))))
            .build();
    assertEquals(expectedRuleAction, customIpBasedRuleList.get(4).getAction());
  }

  @Test
  void getCustomIpBasedRulesTestWithEnvironment() {
    doReturn(sampleGetAllIpRangeRulesResponse)
        .when(ipRangeConfigServiceStub)
        .getIpRangeRules(
            GetIpRangeRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .setDisabled(false)
                        .setRuleScope(
                            RuleScope.newBuilder()
                                .setEnvironmentScope(
                                    EnvironmentScope.newBuilder()
                                        .addEnvironmentIds(ENVIRONMENT_ID))))
                .build());

    List<BlockingPolicyData> customIpBasedRuleList =
        customIpBasedDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder()
                    .environmentId(Optional.of(ENVIRONMENT_ID))
                    .build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList();
    assertEquals(6, customIpBasedRuleList.size());

    // Without environment
    assertThrows(
        NullPointerException.class,
        () ->
            customIpBasedDataFetcher.getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build(),
                mock(BlockingRulesSupplier.class)));

    // Wrong environment
    assertThrows(
        NullPointerException.class,
        () ->
            customIpBasedDataFetcher.getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder()
                    .environmentId(Optional.of(ENVIRONMENT_ID + "random"))
                    .build(),
                mock(BlockingRulesSupplier.class)));
  }

  private static final GetIpRangeRulesResponse sampleGetIpRangeRulesAllEnvsResponse =
      GetIpRangeRulesResponse.newBuilder()
          .addRules(
              IpRangeRule.newBuilder()
                  .setId("rule-id-1")
                  .setRuleDetails(
                      IpRangeRuleDetails.newBuilder()
                          .setName("rule-name-1")
                          .setRuleAction(RULE_ACTION_BLOCK)
                          .setExpirationDetails(
                              ExpirationDetails.newBuilder()
                                  .setExpirationTimestampMillis(activeTimestamp)
                                  .build())
                          .build())
                  .addIpAddresses("1.2.3.4")
                  .addIpAddresses("11.22.33.44")
                  .build())
          .addRules(
              IpRangeRule.newBuilder()
                  .setId("rule-id-2")
                  .setRuleDetails(
                      IpRangeRuleDetails.newBuilder()
                          .setName("rule-name-2")
                          .setRuleAction(RULE_ACTION_BLOCK)
                          .setExpirationDetails(
                              ExpirationDetails.newBuilder()
                                  .setExpirationTimestampMillis(inactiveTimestamp)
                                  .build())
                          .build())
                  .addIpAddresses("1.2.3.4")
                  .addIpAddresses("11.22.33.44")
                  .build())
          .addRules(
              IpRangeRule.newBuilder()
                  .setId("rule-id-3")
                  .setRuleDetails(
                      IpRangeRuleDetails.newBuilder()
                          .setName("rule-name-3")
                          .setRuleAction(RULE_ACTION_BLOCK)
                          .setExpirationDetails(
                              ExpirationDetails.newBuilder()
                                  .setExpirationTimestampMillis(activeTimestamp)
                                  .build())
                          .build())
                  .addIpRanges("1.2.3.4")
                  .build())
          .addRules(
              IpRangeRule.newBuilder()
                  .setId("rule-id-4")
                  .setRuleDetails(
                      IpRangeRuleDetails.newBuilder()
                          .setName("rule-name-4")
                          .setRuleAction(RULE_ACTION_BLOCK)
                          .setExpirationDetails(
                              ExpirationDetails.newBuilder()
                                  .setExpirationTimestampMillis(activeTimestamp))))
          .addRules(
              IpRangeRule.newBuilder()
                  .setId("rule-id-5")
                  .setRuleDetails(
                      IpRangeRuleDetails.newBuilder()
                          .setName("rule-name-5")
                          .setRuleAction(RULE_ACTION_ALLOW)
                          .setExpirationDetails(
                              ExpirationDetails.newBuilder()
                                  .setExpirationTimestampMillis(activeTimestamp)
                                  .build())
                          .build())
                  .addIpAddresses("1.2.3.4")
                  .addIpAddresses("11.22.33.44")
                  .build())
          .addRules(
              IpRangeRule.newBuilder()
                  .setId("rule-id-6")
                  .setRuleDetails(
                      IpRangeRuleDetails.newBuilder()
                          .setName("rule-name-6")
                          .setRuleAction(RULE_ACTION_ALLOW)
                          .setExpirationDetails(
                              ExpirationDetails.newBuilder()
                                  .setExpirationTimestampMillis(inactiveTimestamp)
                                  .build())
                          .build())
                  .addIpAddresses("1.2.3.4")
                  .addIpAddresses("11.22.33.44")
                  .build())
          .addRules(
              IpRangeRule.newBuilder()
                  .setId("rule-id-7")
                  .setRuleDetails(
                      IpRangeRuleDetails.newBuilder()
                          .setName("rule-name-7")
                          .setRuleAction(RULE_ACTION_BLOCK_ALL_EXCEPT)
                          .setExpirationDetails(
                              ExpirationDetails.newBuilder()
                                  .setExpirationTimestampMillis(activeTimestamp)
                                  .build())
                          .build())
                  .addIpAddresses("1.2.3.4")
                  .addIpAddresses("11.22.33.44")
                  .build())
          .addRules(
              IpRangeRule.newBuilder()
                  .setId("rule-id-8")
                  .setRuleDetails(
                      IpRangeRuleDetails.newBuilder()
                          .setName("rule-name-8")
                          .setRuleAction(RULE_ACTION_ALERT)
                          .addEffects(
                              RuleEffectWithModifications.newBuilder()
                                  .setAgentRuleEffect(
                                      AgentRuleEffect.newBuilder()
                                          .addAgentModifications(
                                              AgentModification.newBuilder()
                                                  .setHeaderInjection(
                                                      HeaderInjection.newBuilder()
                                                          .setHeaderLocation(
                                                              PredicateLocation
                                                                  .PREDICATE_LOCATION_REQUEST)
                                                          .setHeaderName("test-header")
                                                          .setValue(
                                                              FieldValue.newBuilder()
                                                                  .setStaticValue("test-value"))))))
                          .setExpirationDetails(
                              ExpirationDetails.newBuilder()
                                  .setExpirationTimestampMillis(activeTimestamp)
                                  .build())
                          .build())
                  .addIpRanges("5.6.7.8")
                  .build())
          .build();

  private static final GetIpRangeRulesResponse sampleGetAllIpRangeRulesResponse =
      sampleGetIpRangeRulesAllEnvsResponse.toBuilder()
          .addRules(
              IpRangeRule.newBuilder()
                  .setId("rule-id-8")
                  .setRuleDetails(
                      IpRangeRuleDetails.newBuilder()
                          .setName("rule-name-8")
                          .setRuleAction(RULE_ACTION_BLOCK)
                          .setExpirationDetails(
                              ExpirationDetails.newBuilder()
                                  .setExpirationTimestampMillis(activeTimestamp)))
                  .addIpAddresses("1.2.3.4")
                  .addIpAddresses("11.22.33.44")
                  .setRuleScope(
                      RuleScope.newBuilder()
                          .setEnvironmentScope(
                              EnvironmentScope.newBuilder().addEnvironmentIds(ENVIRONMENT_ID))))
          .build();
}
