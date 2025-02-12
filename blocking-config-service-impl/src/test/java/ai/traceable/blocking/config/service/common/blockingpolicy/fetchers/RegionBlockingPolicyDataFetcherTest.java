package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALERT;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALLOW;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.RegionBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.v2.AttributeScope;
import ai.traceable.blocking.config.service.v2.InlineModification;
import ai.traceable.blocking.config.service.v2.RuleAction;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.region.config.service.v1.AgentModification;
import ai.traceable.region.config.service.v1.AgentRuleEffect;
import ai.traceable.region.config.service.v1.Country;
import ai.traceable.region.config.service.v1.FieldValue;
import ai.traceable.region.config.service.v1.HeaderInjection;
import ai.traceable.region.config.service.v1.PredicateLocation;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RuleEffectWithModifications;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegionBlockingPolicyDataFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private RegionBlockingPolicyDataFetcher regionDataFetcher;
  private BlockingRulesSupplier blockingRulesSupplier;

  private static final long inactiveTimestamp = System.currentTimeMillis() - 10000L;
  private static final long activeTimestamp = System.currentTimeMillis() + 10000L;

  @BeforeEach
  void setUp() {
    blockingRulesSupplier = mock(BlockingRulesSupplier.class);
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

    regionDataFetcher = new RegionBlockingPolicyDataFetcher(blockingRulesUtils);
  }

  @Test
  void getRegionBasedRulesTestEmpty() {
    doReturn(List.of()).when(blockingRulesSupplier).getRegionRules();
    List<BlockingPolicyData> regionBasedRuleList =
        regionDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build(),
                blockingRulesSupplier)
            .getBlockingPolicyList();
    assertEquals(0, regionBasedRuleList.size());
  }

  @Test
  void getRegionBasedRulesTest() {
    doReturn(sampleRegionAllEnvRulesResponse).when(blockingRulesSupplier).getRegionRules();

    List<BlockingPolicyData> regionBasedRuleList =
        regionDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build(),
                blockingRulesSupplier)
            .getBlockingPolicyList();

    assertEquals(3, regionBasedRuleList.size());
    assertEquals(
        RegionBlockingDetails.builder().regions(List.of("NP", "BN")).build(),
        regionBasedRuleList.get(0).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_REGION_RULE, regionBasedRuleList.get(0).getCategory());
    assertEquals(BlockingPolicyData.RuleType.BLOCK, regionBasedRuleList.get(0).getRuleType());
    assertEquals(BlockingPolicyData.Status.SUSPENDED, regionBasedRuleList.get(0).getStatus());
    assertEquals(activeTimestamp, regionBasedRuleList.get(0).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo("rule-id-1", "rule-name-1"),
        regionBasedRuleList.get(0).getInfo());
    assertEquals("rule-id-1", regionBasedRuleList.get(0).getRuleId());

    assertEquals(
        RegionBlockingDetails.builder().regions(List.of("CH", "PK")).build(),
        regionBasedRuleList.get(1).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_REGION_RULE, regionBasedRuleList.get(1).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.BLOCK_ALL_EXCEPT, regionBasedRuleList.get(1).getRuleType());
    assertEquals(BlockingPolicyData.Status.DENIED, regionBasedRuleList.get(1).getStatus());
    assertEquals(0, regionBasedRuleList.get(1).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo("rule-id-3", "rule-name-3"),
        regionBasedRuleList.get(1).getInfo());
    assertEquals("rule-id-3", regionBasedRuleList.get(1).getRuleId());

    assertEquals(
        RegionBlockingDetails.builder().regions(List.of("ML", "SN")).build(),
        regionBasedRuleList.get(2).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_REGION_RULE, regionBasedRuleList.get(2).getCategory());
    assertEquals(BlockingPolicyData.RuleType.ANALYTICS, regionBasedRuleList.get(2).getRuleType());
    assertNull(regionBasedRuleList.get(2).getStatus());
    assertEquals(activeTimestamp, regionBasedRuleList.get(2).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo("rule-id-6", "rule-name-6"),
        regionBasedRuleList.get(2).getInfo());
    assertEquals("rule-id-6", regionBasedRuleList.get(2).getRuleId());
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
    assertEquals(expectedRuleAction, regionBasedRuleList.get(2).getAction());
  }

  private static final List<RegionRule> sampleRegionAllEnvRulesResponse =
      List.of(
          RegionRule.newBuilder()
              .setId("rule-id-1")
              .addAllRegionId(List.of("Nepal", "Bhutan"))
              .setName("rule-name-1")
              .setActionType(REGION_RULE_ACTION_TYPE_BLOCK)
              .setExpirationDetails(
                  RegionRule.ExpirationDetails.newBuilder()
                      .setTimestampMillis(activeTimestamp)
                      .build())
              .putRegionIdToCountryMap("Nepal", Country.newBuilder().setIsoCode("NP").build())
              .putRegionIdToCountryMap("Bhutan", Country.newBuilder().setIsoCode("BN").build())
              .build(),
          RegionRule.newBuilder()
              .setId("rule-id-2")
              .addAllRegionId(List.of("Nepal", "Bhutan"))
              .setName("rule-name-2")
              .setActionType(REGION_RULE_ACTION_TYPE_ALLOW)
              .setExpirationDetails(
                  RegionRule.ExpirationDetails.newBuilder()
                      .setTimestampMillis(activeTimestamp)
                      .build())
              .putRegionIdToCountryMap("Nepal", Country.newBuilder().setIsoCode("NP").build())
              .putRegionIdToCountryMap("Bhutan", Country.newBuilder().setIsoCode("BN").build())
              .build(),
          RegionRule.newBuilder()
              .setId("rule-id-3")
              .addAllRegionId(List.of("China", "Pakistan"))
              .setName("rule-name-3")
              .setActionType(REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
              .setExpirationDetails(RegionRule.ExpirationDetails.newBuilder().setTimestampMillis(0))
              .putRegionIdToCountryMap("China", Country.newBuilder().setIsoCode("CH").build())
              .putRegionIdToCountryMap("Pakistan", Country.newBuilder().setIsoCode("PK").build())
              .build(),
          RegionRule.newBuilder()
              .setId("rule-id-4")
              .addAllRegionId(List.of("Nepal", "Bhutan"))
              .setName("rule-name-4")
              .setActionType(REGION_RULE_ACTION_TYPE_BLOCK)
              .setExpirationDetails(
                  RegionRule.ExpirationDetails.newBuilder().setTimestampMillis(inactiveTimestamp))
              .putRegionIdToCountryMap("Nepal", Country.newBuilder().setIsoCode("NP").build())
              .putRegionIdToCountryMap("Bhutan", Country.newBuilder().setIsoCode("BN").build())
              .build(),
          RegionRule.newBuilder()
              .setId("rule-id-5")
              .addAllRegionId(List.of("Nepal", "Bhutan"))
              .setName("rule-name-5")
              .setActionType(REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
              .setExpirationDetails(
                  RegionRule.ExpirationDetails.newBuilder().setTimestampMillis(inactiveTimestamp))
              .putRegionIdToCountryMap("Nepal", Country.newBuilder().setIsoCode("NP").build())
              .putRegionIdToCountryMap("Bhutan", Country.newBuilder().setIsoCode("BN").build())
              .build(),
          RegionRule.newBuilder()
              .setId("rule-id-6")
              .addAllRegionId(List.of("Malaysia", "Singapore"))
              .setName("rule-name-6")
              .setActionType(REGION_RULE_ACTION_TYPE_ALERT)
              .addEffects(
                  RuleEffectWithModifications.newBuilder()
                      .setAgentRuleEffect(
                          AgentRuleEffect.newBuilder()
                              .addAgentModifications(
                                  AgentModification.newBuilder()
                                      .setHeaderInjection(
                                          HeaderInjection.newBuilder()
                                              .setHeaderLocation(
                                                  PredicateLocation.PREDICATE_LOCATION_REQUEST)
                                              .setHeaderName("test-header")
                                              .setValue(
                                                  FieldValue.newBuilder()
                                                      .setStaticValue("test-value"))))))
              .setExpirationDetails(
                  RegionRule.ExpirationDetails.newBuilder().setTimestampMillis(activeTimestamp))
              .putRegionIdToCountryMap("Malaysia", Country.newBuilder().setIsoCode("ML").build())
              .putRegionIdToCountryMap("Singapore", Country.newBuilder().setIsoCode("SN").build())
              .build());
}
