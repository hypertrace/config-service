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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.impl.CustomIpBasedDataFetcherImpl;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.iprange.config.service.v1.ExpirationDetails;
import ai.traceable.iprange.config.service.v1.GetIpRangeRulesRequest;
import ai.traceable.iprange.config.service.v1.GetIpRangeRulesResponse;
import ai.traceable.iprange.config.service.v1.GetRulesFilter;
import ai.traceable.iprange.config.service.v1.IpRangeConfigServiceGrpc.IpRangeConfigServiceBlockingStub;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.iprange.config.service.v1.IpRangeRuleDetails;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CustomIpBasedDataFetcherTest {

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
        new CustomIpBasedDataFetcherImpl(ipRangeConfigServiceStub, blockingRulesUtils);
  }

  @Test
  void getCustomIpBasedViolations() {
    doReturn(
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
                .build())
        .when(ipRangeConfigServiceStub)
        .getIpRangeRules(
            GetIpRangeRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .setDisabled(false)
                        .setRuleAction(RULE_ACTION_BLOCK)
                        .build())
                .build());

    List<BlockingDetails> violations = customIpBasedDataFetcher.getCustomIpBasedViolations();
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
  }

  @Test
  void getCustomIpBasedExemptions() {
    doReturn(
            GetIpRangeRulesResponse.newBuilder()
                .addRules(
                    IpRangeRule.newBuilder()
                        .setId("rule-id-1")
                        .setRuleDetails(
                            IpRangeRuleDetails.newBuilder()
                                .setName("rule-name-1")
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
                        .setId("rule-id-2")
                        .setRuleDetails(
                            IpRangeRuleDetails.newBuilder()
                                .setName("rule-name-2")
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
                        .setId("rule-id-3")
                        .setRuleDetails(
                            IpRangeRuleDetails.newBuilder()
                                .setName("rule-name-3")
                                .setRuleAction(RULE_ACTION_ALLOW)
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
                                .setRuleAction(RULE_ACTION_ALLOW)
                                .setExpirationDetails(
                                    ExpirationDetails.newBuilder()
                                        .setExpirationTimestampMillis(activeTimestamp)
                                        .build())
                                .build())
                        .build())
                .build())
        .when(ipRangeConfigServiceStub)
        .getIpRangeRules(
            GetIpRangeRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .setDisabled(false)
                        .setRuleAction(RULE_ACTION_ALLOW)
                        .build())
                .build());

    List<BlockingDetails> exemptions = customIpBasedDataFetcher.getCustomIpBasedExemptions();
    assertEquals(2, exemptions.size());
    assertEquals("1.2.3.4", exemptions.get(0).getIpDetails().getIpAddresses(0));
    assertEquals("11.22.33.44", exemptions.get(0).getIpDetails().getIpAddresses(1));
    assertEquals("1.2.3.4", exemptions.get(1).getIpDetails().getIpRanges(0));
    assertEquals(BLOCKING_CATEGORY_CUSTOM_IP_RULE, exemptions.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_ALLOW, exemptions.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_ALLOWED, exemptions.get(0).getStatus());
    assertEquals(
        ExemptionInfoEncoder.getEncodedCustomIpRuleExemptionInfo("rule-id-1", "rule-name-1"),
        exemptions.get(0).getInfo());
  }

  @Test
  void getCustomIpBasedBlockAllExcepts() {
    doReturn(
            GetIpRangeRulesResponse.newBuilder()
                .addRules(
                    IpRangeRule.newBuilder()
                        .setId("rule-id-1")
                        .setRuleDetails(
                            IpRangeRuleDetails.newBuilder()
                                .setName("rule-name-1")
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
                        .setId("rule-id-2")
                        .setRuleDetails(
                            IpRangeRuleDetails.newBuilder()
                                .setName("rule-name-2")
                                .setRuleAction(RULE_ACTION_BLOCK_ALL_EXCEPT)
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
                                .setRuleAction(RULE_ACTION_BLOCK_ALL_EXCEPT)
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
                                .setRuleAction(RULE_ACTION_BLOCK_ALL_EXCEPT)
                                .setExpirationDetails(
                                    ExpirationDetails.newBuilder()
                                        .setExpirationTimestampMillis(activeTimestamp)
                                        .build())
                                .build())
                        .build())
                .build())
        .when(ipRangeConfigServiceStub)
        .getIpRangeRules(
            GetIpRangeRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .setDisabled(false)
                        .setRuleAction(RULE_ACTION_BLOCK_ALL_EXCEPT)
                        .build())
                .build());

    List<BlockingDetails> blockAllExcepts =
        customIpBasedDataFetcher.getCustomIpBasedBlockAllExcepts();
    assertEquals(2, blockAllExcepts.size());
    assertEquals("1.2.3.4", blockAllExcepts.get(0).getIpDetails().getIpAddresses(0));
    assertEquals("11.22.33.44", blockAllExcepts.get(0).getIpDetails().getIpAddresses(1));
    assertEquals("1.2.3.4", blockAllExcepts.get(1).getIpDetails().getIpRanges(0));
    assertEquals(BLOCKING_CATEGORY_CUSTOM_IP_RULE, blockAllExcepts.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT, blockAllExcepts.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_DENIED, blockAllExcepts.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomIpRuleViolationInfo("rule-id-1", "rule-name-1"),
        blockAllExcepts.get(0).getInfo());
  }
}
