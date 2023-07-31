package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_ALLOW;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK;
import static ai.traceable.region.config.service.v1.RegionRuleActionType.REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.RegionBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.region.config.service.v1.EnvironmentScope;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetAllRegionRulesResponse;
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RuleScope;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegionBlockingPolicyDataFetcherTest {

  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final GetAllRegionRulesRequest DEFAULT_GET_REQUEST =
      GetAllRegionRulesRequest.newBuilder()
          .setFilter(
              GetRegionRulesFilter.newBuilder()
                  .setRuleScope(
                      RuleScope.newBuilder()
                          .setEnvironmentScope(EnvironmentScope.getDefaultInstance()))
                  .addAllRuleActionTypes(
                      List.of(
                          REGION_RULE_ACTION_TYPE_BLOCK, REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT))
                  .setDisabled(false))
          .build();

  private RegionConfigServiceGrpc.RegionConfigServiceBlockingStub regionConfigServiceBlockingStub;

  private RegionBlockingPolicyDataFetcher regionDataFetcher;

  private static final long inactiveTimestamp = System.currentTimeMillis() - 10000L;
  private static final long activeTimestamp = System.currentTimeMillis() + 10000L;

  @BeforeEach
  void setUp() {
    regionConfigServiceBlockingStub =
        mock(RegionConfigServiceGrpc.RegionConfigServiceBlockingStub.class);

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

    regionDataFetcher =
        new RegionBlockingPolicyDataFetcher(regionConfigServiceBlockingStub, blockingRulesUtils);
  }

  @Test
  void getRegionBasedRulesTestEmpty() {
    doReturn(GetAllRegionRulesResponse.getDefaultInstance())
        .when(regionConfigServiceBlockingStub)
        .getAllRegionRules(DEFAULT_GET_REQUEST);
    List<BlockingPolicyData> regionBasedRuleList =
        regionDataFetcher.getBlockingPolicyData(
            REQUEST_CONTEXT,
            BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build());
    assertEquals(0, regionBasedRuleList.size());
  }

  @Test
  void getRegionBasedRulesTestWithoutEnvironment() {
    doReturn(sampleRegionAllEnvRulesResponse)
        .when(regionConfigServiceBlockingStub)
        .getAllRegionRules(DEFAULT_GET_REQUEST);

    List<BlockingPolicyData> regionBasedRuleList =
        regionDataFetcher.getBlockingPolicyData(
            REQUEST_CONTEXT,
            BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build());

    assertEquals(2, regionBasedRuleList.size());
    assertEquals(
        RegionBlockingDetails.builder().regions(List.of("Nepal", "Bhutan")).build(),
        regionBasedRuleList.get(0).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_REGION_RULE, regionBasedRuleList.get(0).getCategory());
    assertEquals(BlockingPolicyData.RuleType.BLOCK, regionBasedRuleList.get(0).getRuleType());
    assertEquals(BlockingPolicyData.Status.SUSPENDED, regionBasedRuleList.get(0).getStatus());
    assertEquals(activeTimestamp, regionBasedRuleList.get(0).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomRegionRuleViolationInfo("rule-id-1", "rule-name-1"),
        regionBasedRuleList.get(0).getInfo());

    assertEquals(
        RegionBlockingDetails.builder().regions(List.of("China", "Pakistan")).build(),
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
                        .addAllRuleActionTypes(
                            List.of(
                                REGION_RULE_ACTION_TYPE_BLOCK,
                                REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT))
                        .build())
                .build());

    // With correct environment
    List<BlockingPolicyData> regionBasedRuleList =
        regionDataFetcher.getBlockingPolicyData(
            REQUEST_CONTEXT,
            BlockingPolicyDataFilter.builder().environmentId(Optional.of(ENVIRONMENT_ID)).build());
    assertEquals(3, regionBasedRuleList.size());

    // Without environment
    assertThrows(
        NullPointerException.class,
        () ->
            regionDataFetcher.getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build()));

    // Wrong environment
    assertThrows(
        NullPointerException.class,
        () ->
            regionDataFetcher.getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder()
                    .environmentId(Optional.of(ENVIRONMENT_ID + "random"))
                    .build()));
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
                      RegionRule.ExpirationDetails.newBuilder()
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
                      RegionRule.ExpirationDetails.newBuilder()
                          .setTimestampMillis(activeTimestamp)
                          .build())
                  .build())
          .addRule(
              RegionRule.newBuilder()
                  .setId("rule-id-3")
                  .addAllRegionId(List.of("China", "Pakistan"))
                  .setName("rule-name-3")
                  .setActionType(REGION_RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)
                  .setExpirationDetails(
                      RegionRule.ExpirationDetails.newBuilder().setTimestampMillis(0).build())
                  .build())
          .addRule(
              RegionRule.newBuilder()
                  .setId("rule-id-4")
                  .addAllRegionId(List.of("Nepal", "Bhutan"))
                  .setName("rule-name-4")
                  .setActionType(REGION_RULE_ACTION_TYPE_BLOCK)
                  .setExpirationDetails(
                      RegionRule.ExpirationDetails.newBuilder()
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
                      RegionRule.ExpirationDetails.newBuilder()
                          .setTimestampMillis(inactiveTimestamp)
                          .build())
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
                      RegionRule.ExpirationDetails.newBuilder()
                          .setTimestampMillis(activeTimestamp)
                          .build())
                  .setRuleScope(
                      RuleScope.newBuilder()
                          .setEnvironmentScope(
                              EnvironmentScope.newBuilder().addEnvironmentIds(ENVIRONMENT_ID))))
          .build();
}
