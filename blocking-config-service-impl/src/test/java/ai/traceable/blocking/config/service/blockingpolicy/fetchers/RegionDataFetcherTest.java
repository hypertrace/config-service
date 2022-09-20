package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_REGION_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_DENIED;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALLOW;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingRuleType;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.region.config.service.v1.EnvironmentScope;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetAllRegionRulesResponse;
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionRule.ExpirationDetails;
import ai.traceable.region.config.service.v1.RuleScope;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegionDataFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final GetAllRegionRulesRequest DEFAULT_GET_REQUEST =
      GetAllRegionRulesRequest.newBuilder()
          .setFilter(
              GetRegionRulesFilter.newBuilder()
                  .setRuleScope(RuleScope.getDefaultInstance())
                  .setDisabled(false))
          .build();

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

    regionDataFetcher = new RegionDataFetcher(regionConfigServiceBlockingStub, blockingRulesUtils);
  }

  @Test
  void getRegionBasedRulesTestEmpty() {
    doReturn(GetAllRegionRulesResponse.getDefaultInstance())
        .when(regionConfigServiceBlockingStub)
        .getAllRegionRules(DEFAULT_GET_REQUEST);

    Map<BlockingRuleType, List<BlockingDetails>> regionBasedRuleMap =
        regionDataFetcher.getRegionBasedRules(REQUEST_CONTEXT, Optional.empty());

    assertEquals(2, regionBasedRuleMap.size());
    assertEquals(0, regionBasedRuleMap.get(BLOCKING_RULE_TYPE_BLOCK).size());
    assertEquals(0, regionBasedRuleMap.get(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT).size());
  }

  @Test
  void getRegionBasedRulesTestWithoutEnvironment() {
    doReturn(sampleRegionAllEnvRulesResponse)
        .when(regionConfigServiceBlockingStub)
        .getAllRegionRules(DEFAULT_GET_REQUEST);

    Map<BlockingRuleType, List<BlockingDetails>> regionBasedRuleMap =
        regionDataFetcher.getRegionBasedRules(REQUEST_CONTEXT, Optional.empty());

    assertEquals(2, regionBasedRuleMap.size());

    List<BlockingDetails> violations = regionBasedRuleMap.get(BLOCKING_RULE_TYPE_BLOCK);
    assertEquals(1, violations.size());
    assertEquals(List.of("Nepal", "Bhutan"), violations.get(0).getRegionDetails().getRegionsList());
    assertEquals(BLOCKING_CATEGORY_CUSTOM_REGION_RULE, violations.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_BLOCK, violations.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_DENIED, violations.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo("rule-id-1", "rule-name-1"),
        violations.get(0).getInfo());

    List<BlockingDetails> blockAllExcepts =
        regionBasedRuleMap.get(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT);
    assertEquals(1, blockAllExcepts.size());
    assertEquals(
        List.of("China", "Pakistan"), blockAllExcepts.get(0).getRegionDetails().getRegionsList());
    assertEquals(BLOCKING_CATEGORY_CUSTOM_REGION_RULE, blockAllExcepts.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT, blockAllExcepts.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_DENIED, blockAllExcepts.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo("rule-id-3", "rule-name-3"),
        blockAllExcepts.get(0).getInfo());
  }

  @Test
  void getRegionBasedRulesTestWithEnvironment() {
    doReturn(sampleRegionAllRulesResponse)
        .when(regionConfigServiceBlockingStub)
        .getAllRegionRules(
            GetAllRegionRulesRequest.newBuilder()
                .setFilter(
                    GetRegionRulesFilter.newBuilder()
                        .setRuleScope(
                            RuleScope.newBuilder()
                                .setEnvironmentScope(
                                    EnvironmentScope.newBuilder()
                                        .addEnvironmentIds(ENVIRONMENT_ID)
                                        .build())
                                .build())
                        .setDisabled(false)
                        .build())
                .build());

    // With correct environment
    Map<BlockingRuleType, List<BlockingDetails>> regionBasedRuleMap =
        regionDataFetcher.getRegionBasedRules(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));
    assertEquals(2, regionBasedRuleMap.size());
    assertEquals(2, regionBasedRuleMap.get(BLOCKING_RULE_TYPE_BLOCK).size());
    assertEquals(1, regionBasedRuleMap.get(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT).size());

    // Without environment
    assertThrows(
        NullPointerException.class,
        () -> regionDataFetcher.getRegionBasedRules(REQUEST_CONTEXT, Optional.empty()));

    // Wrong environment
    assertThrows(
        NullPointerException.class,
        () ->
            regionDataFetcher.getRegionBasedRules(
                REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID + "random")));
  }

  private static final GetAllRegionRulesResponse sampleRegionAllEnvRulesResponse =
      GetAllRegionRulesResponse.newBuilder()
          .addRule(
              RegionRule.newBuilder()
                  .setId("rule-id-1")
                  .addAllRegionId(List.of("Nepal", "Bhutan"))
                  .setName("rule-name-1")
                  .setActionType(REGION_RULE_ACTION_TYPE_BLOCK)
                  .setExpirationDetails(
                      ExpirationDetails.newBuilder().setTimestampMillis(activeTimestamp).build())
                  .build())
          .addRule(
              RegionRule.newBuilder()
                  .setId("rule-id-2")
                  .addAllRegionId(List.of("Nepal", "Bhutan"))
                  .setName("rule-name-2")
                  .setActionType(REGION_RULE_ACTION_TYPE_ALLOW)
                  .setExpirationDetails(
                      ExpirationDetails.newBuilder().setTimestampMillis(activeTimestamp).build())
                  .build())
          .addRule(
              RegionRule.newBuilder()
                  .setId("rule-id-3")
                  .addAllRegionId(List.of("China", "Pakistan"))
                  .setName("rule-name-3")
                  .setActionType(REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                  .setExpirationDetails(
                      ExpirationDetails.newBuilder().setTimestampMillis(activeTimestamp).build())
                  .build())
          .addRule(
              RegionRule.newBuilder()
                  .setId("rule-id-4")
                  .addAllRegionId(List.of("Nepal", "Bhutan"))
                  .setName("rule-name-4")
                  .setActionType(REGION_RULE_ACTION_TYPE_BLOCK)
                  .setExpirationDetails(
                      ExpirationDetails.newBuilder().setTimestampMillis(inactiveTimestamp).build())
                  .build())
          .addRule(
              RegionRule.newBuilder()
                  .setId("rule-id-5")
                  .addAllRegionId(List.of("Nepal", "Bhutan"))
                  .setName("rule-name-5")
                  .setActionType(REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                  .setExpirationDetails(
                      ExpirationDetails.newBuilder().setTimestampMillis(inactiveTimestamp).build())
                  .build())
          .build();

  private static final GetAllRegionRulesResponse sampleRegionAllRulesResponse =
      sampleRegionAllEnvRulesResponse.toBuilder()
          .addRule(
              RegionRule.newBuilder()
                  .setId("rule-id-6")
                  .addAllRegionId(List.of("Nepal", "Bhutan"))
                  .setName("rule-name-6")
                  .setActionType(REGION_RULE_ACTION_TYPE_BLOCK)
                  .setExpirationDetails(
                      ExpirationDetails.newBuilder().setTimestampMillis(activeTimestamp).build())
                  .setRuleScope(
                      RuleScope.newBuilder()
                          .setEnvironmentScope(
                              EnvironmentScope.newBuilder().addEnvironmentIds(ENVIRONMENT_ID))))
          .build();
}
