package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_REGION_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_DENIED;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALLOW;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.impl.RegionDataFetcherImpl;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetAllRegionRulesResponse;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRule.ExpirationDetails;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegionDataFetcherTest {

  private RegionConfigServiceBlockingStub regionConfigServiceBlockingStub;
  private RegionDataFetcher regionDataFetcher;

  private static final long inactiveTimestamp = System.currentTimeMillis() - 10000L;
  private static final long activeTimestamp = System.currentTimeMillis() + 10000L;

  @BeforeEach
  void setUp() {
    regionConfigServiceBlockingStub = mock(RegionConfigServiceBlockingStub.class);

    BlockingRulesUtils blockingRulesUtils = mock(BlockingRulesUtils.class);
    doReturn(true).when(blockingRulesUtils).isRuleActive(activeTimestamp);
    doReturn(false).when(blockingRulesUtils).isRuleActive(inactiveTimestamp);

    doReturn(BLOCKING_STATUS_DENIED)
        .when(blockingRulesUtils)
        .generateBlockingStatus(activeTimestamp, BLOCKING_RULE_TYPE_BLOCK);
    doReturn(BLOCKING_STATUS_DENIED)
        .when(blockingRulesUtils)
        .generateBlockingStatus(activeTimestamp, BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT);

    regionDataFetcher =
        new RegionDataFetcherImpl(regionConfigServiceBlockingStub, blockingRulesUtils);
    initMock();
  }

  @Test
  void getRegionViolations() {
    List<BlockingDetails> violations = regionDataFetcher.getRegionViolations();
    assertEquals(1, violations.size());
    assertEquals(List.of("Nepal", "Bhutan"), violations.get(0).getRegionDetails().getRegionsList());
    assertEquals(BLOCKING_CATEGORY_CUSTOM_REGION_RULE, violations.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_BLOCK, violations.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_DENIED, violations.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo("rule-id-1", "rule-name-1"),
        violations.get(0).getInfo());
  }

  @Test
  void getRegionViolationAllExcepts() {
    List<BlockingDetails> blockAllExcepts = regionDataFetcher.getRegionViolationAllExcepts();
    assertEquals(1, blockAllExcepts.size());
    assertEquals(
        List.of("Nepal", "Bhutan"), blockAllExcepts.get(0).getRegionDetails().getRegionsList());
    assertEquals(BLOCKING_CATEGORY_CUSTOM_REGION_RULE, blockAllExcepts.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT, blockAllExcepts.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_DENIED, blockAllExcepts.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo("rule-id-3", "rule-name-3"),
        blockAllExcepts.get(0).getInfo());
  }

  private void initMock() {
    doReturn(
            GetAllRegionRulesResponse.newBuilder()
                .addRule(
                    RegionRule.newBuilder()
                        .setId("rule-id-1")
                        .addAllRegionId(List.of("Nepal", "Bhutan"))
                        .setName("rule-name-1")
                        .setActionType(REGION_RULE_ACTION_TYPE_BLOCK)
                        .setExpirationDetails(
                            ExpirationDetails.newBuilder()
                                .setTimestampMillis(activeTimestamp)
                                .build())
                        .build())
                .addRule(
                    RegionRule.newBuilder()
                        .setId("rule-id-2")
                        .addAllRegionId(List.of("Nepal", "Bhutan"))
                        .setName("rule-name-2")
                        .setActionType(REGION_RULE_ACTION_TYPE_ALLOW)
                        .setExpirationDetails(
                            ExpirationDetails.newBuilder()
                                .setTimestampMillis(activeTimestamp)
                                .build())
                        .build())
                .addRule(
                    RegionRule.newBuilder()
                        .setId("rule-id-3")
                        .addAllRegionId(List.of("Nepal", "Bhutan"))
                        .setName("rule-name-3")
                        .setActionType(REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                        .setExpirationDetails(
                            ExpirationDetails.newBuilder()
                                .setTimestampMillis(activeTimestamp)
                                .build())
                        .build())
                .addRule(
                    RegionRule.newBuilder()
                        .setId("rule-id-4")
                        .addAllRegionId(List.of("Nepal", "Bhutan"))
                        .setName("rule-name-4")
                        .setActionType(REGION_RULE_ACTION_TYPE_BLOCK)
                        .setExpirationDetails(
                            ExpirationDetails.newBuilder()
                                .setTimestampMillis(inactiveTimestamp)
                                .build())
                        .build())
                .addRule(
                    RegionRule.newBuilder()
                        .setId("rule-id-5")
                        .addAllRegionId(List.of("Nepal", "Bhutan"))
                        .setName("rule-name-5")
                        .setActionType(REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                        .setExpirationDetails(
                            ExpirationDetails.newBuilder()
                                .setTimestampMillis(inactiveTimestamp)
                                .build())
                        .build())
                .addRule(
                    RegionRule.newBuilder()
                        .setId("rule-id-6")
                        .addAllRegionId(List.of())
                        .setName("rule-name-6")
                        .setActionType(REGION_RULE_ACTION_TYPE_BLOCK)
                        .setExpirationDetails(
                            ExpirationDetails.newBuilder()
                                .setTimestampMillis(activeTimestamp)
                                .build())
                        .build())
                .build())
        .when(regionConfigServiceBlockingStub)
        .getAllRegionRules(GetAllRegionRulesRequest.getDefaultInstance());
  }
}
