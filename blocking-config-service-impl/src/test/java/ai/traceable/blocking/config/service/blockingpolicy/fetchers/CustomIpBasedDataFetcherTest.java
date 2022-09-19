package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_IP_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_ALLOWED;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_DENIED;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_ALLOW;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_BLOCK;
import static ai.traceable.iprange.config.service.v1.RuleAction.RULE_ACTION_BLOCK_ALL_EXCEPT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingRuleType;
import ai.traceable.iprange.config.service.v1.EnvironmentScope;
import ai.traceable.iprange.config.service.v1.ExpirationDetails;
import ai.traceable.iprange.config.service.v1.GetIpRangeRulesRequest;
import ai.traceable.iprange.config.service.v1.GetIpRangeRulesResponse;
import ai.traceable.iprange.config.service.v1.GetRulesFilter;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.iprange.config.service.v1.IpRangeRuleDetails;
import ai.traceable.iprange.config.service.v1.RuleScope;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CustomIpBasedDataFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final GetIpRangeRulesRequest DEFAULT_GET_REQUEST =
      GetIpRangeRulesRequest.newBuilder()
          .setFilter(
              GetRulesFilter.newBuilder()
                  .setDisabled(false)
                  .setRuleScope(
                      RuleScope.newBuilder().setEnvironmentScope(EnvironmentScope.newBuilder()))
                  .build())
          .build();

  private IpRangeConfigServiceBlockingStub ipRangeConfigServiceStub;
  private CustomIpBasedDataFetcher customIpBasedDataFetcher;

  private static final long inactiveTimestamp = System.currentTimeMillis() - 10000L;
  private static final long activeTimestamp = System.currentTimeMillis() + 10000L;

  @BeforeEach
  void setUp() {
    ipRangeConfigServiceStub = mock(IpRangeConfigServiceBlockingStub.class);

    BlockingRulesUtils blockingRulesUtils = mock(BlockingRulesUtils.class);
    doReturn(true).when(blockingRulesUtils).isRuleActive(activeTimestamp);
    doReturn(false).when(blockingRulesUtils).isRuleActive(inactiveTimestamp);

    doReturn(BLOCKING_STATUS_ALLOWED)
        .when(blockingRulesUtils)
        .generateBlockingStatus(activeTimestamp, BLOCKING_RULE_TYPE_ALLOW);
    doReturn(BLOCKING_STATUS_DENIED)
        .when(blockingRulesUtils)
        .generateBlockingStatus(activeTimestamp, BLOCKING_RULE_TYPE_BLOCK);
    doReturn(BLOCKING_STATUS_DENIED)
        .when(blockingRulesUtils)
        .generateBlockingStatus(activeTimestamp, BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT);

    customIpBasedDataFetcher =
        new CustomIpBasedDataFetcher(ipRangeConfigServiceStub, blockingRulesUtils);
  }

  @Test
  void getCustomIpBasedRulesTestEmpty() {
    doReturn(GetIpRangeRulesResponse.getDefaultInstance())
        .when(ipRangeConfigServiceStub)
        .getIpRangeRules(DEFAULT_GET_REQUEST);

    Map<BlockingRuleType, List<BlockingDetails>> customIpBasedRuleMap =
        customIpBasedDataFetcher.getCustomIpBasedRules(REQUEST_CONTEXT, Optional.empty());

    assertEquals(3, customIpBasedRuleMap.size());
    assertEquals(0, customIpBasedRuleMap.get(BLOCKING_RULE_TYPE_ALLOW).size());
    assertEquals(0, customIpBasedRuleMap.get(BLOCKING_RULE_TYPE_BLOCK).size());
    assertEquals(0, customIpBasedRuleMap.get(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT).size());
  }

  @Test
  void getCustomIpBasedRulesTestWithoutEnvironment() {
    doReturn(sampleGetIpRangeRulesAllEnvsResponse)
        .when(ipRangeConfigServiceStub)
        .getIpRangeRules(DEFAULT_GET_REQUEST);

    Map<BlockingRuleType, List<BlockingDetails>> customIpBasedRuleMap =
        customIpBasedDataFetcher.getCustomIpBasedRules(REQUEST_CONTEXT, Optional.empty());

    assertEquals(3, customIpBasedRuleMap.size());

    List<BlockingDetails> violations = customIpBasedRuleMap.get(BLOCKING_RULE_TYPE_BLOCK);
    assertEquals(2, violations.size());
    assertEquals("1.2.3.4", violations.get(0).getIpDetails().getIpAddresses(0));
    assertEquals("11.22.33.44", violations.get(0).getIpDetails().getIpAddresses(1));
    assertEquals("1.2.3.4", violations.get(1).getIpDetails().getIpRanges(0));
    assertEquals(BLOCKING_CATEGORY_CUSTOM_IP_RULE, violations.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_BLOCK, violations.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_DENIED, violations.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomIpRuleViolationInfo("rule-id-1", "rule-name-1"),
        violations.get(0).getInfo());

    List<BlockingDetails> exemptions = customIpBasedRuleMap.get(BLOCKING_RULE_TYPE_ALLOW);
    assertEquals(1, exemptions.size());
    assertEquals("1.2.3.4", exemptions.get(0).getIpDetails().getIpAddresses(0));
    assertEquals("11.22.33.44", exemptions.get(0).getIpDetails().getIpAddresses(1));
    assertEquals(BLOCKING_CATEGORY_CUSTOM_IP_RULE, exemptions.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_ALLOW, exemptions.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_ALLOWED, exemptions.get(0).getStatus());
    assertEquals(
        ExemptionInfoEncoder.getEncodedCustomIpRuleExemptionInfo("rule-id-5", "rule-name-5"),
        exemptions.get(0).getInfo());

    List<BlockingDetails> blockAllExcepts =
        customIpBasedRuleMap.get(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT);
    assertEquals(1, blockAllExcepts.size());
    assertEquals("1.2.3.4", blockAllExcepts.get(0).getIpDetails().getIpAddresses(0));
    assertEquals("11.22.33.44", blockAllExcepts.get(0).getIpDetails().getIpAddresses(1));
    assertEquals(BLOCKING_CATEGORY_CUSTOM_IP_RULE, blockAllExcepts.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT, blockAllExcepts.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_DENIED, blockAllExcepts.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomIpRuleViolationInfo("rule-id-7", "rule-name-7"),
        blockAllExcepts.get(0).getInfo());
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
                                        .addEnvironmentIds(ENVIRONMENT_ID)
                                        .build())
                                .build())
                        .build())
                .build());

    Map<BlockingRuleType, List<BlockingDetails>> customIpBasedRuleMap =
        customIpBasedDataFetcher.getCustomIpBasedRules(
            REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    assertEquals(3, customIpBasedRuleMap.size());
    assertEquals(3, customIpBasedRuleMap.get(BLOCKING_RULE_TYPE_BLOCK).size());
    assertEquals(1, customIpBasedRuleMap.get(BLOCKING_RULE_TYPE_ALLOW).size());
    assertEquals(1, customIpBasedRuleMap.get(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT).size());

    // Without environment
    assertThrows(
        NullPointerException.class,
        () -> customIpBasedDataFetcher.getCustomIpBasedRules(REQUEST_CONTEXT, Optional.empty()));

    // Wrong environment
    assertThrows(
        NullPointerException.class,
        () ->
            customIpBasedDataFetcher.getCustomIpBasedRules(
                REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID + "random")));
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
                                  .setExpirationTimestampMillis(activeTimestamp)
                                  .build())
                          .build())
                  .build())
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
