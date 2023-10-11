package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.ratelimiting.config.service.v2.Action.EventSeverity.EVENT_SEVERITY_HIGH;
import static ai.traceable.ratelimiting.config.service.v2.Action.EventSeverity.EVENT_SEVERITY_UNSPECIFIED;
import static ai.traceable.ratelimiting.config.service.v2.Category.CATEGORY_DATA_EXFILTRATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CombinationBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CombinationBlockingDetails.Operator;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CustomSignatureBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpTypeBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.RegionBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Action.Allow;
import ai.traceable.ratelimiting.config.service.v2.Action.Block;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition;
import ai.traceable.ratelimiting.config.service.v2.CompositeCondition.LogicalOperator;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.IpAddressCondition;
import ai.traceable.ratelimiting.config.service.v2.IpLocationType;
import ai.traceable.ratelimiting.config.service.v2.IpLocationTypeCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.ModsecRuleIdInfo;
import ai.traceable.ratelimiting.config.service.v2.ModsecRuleIdInfo.IdType;
import ai.traceable.ratelimiting.config.service.v2.ModsecRuleIdInfo.MatchCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingModsecRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition;
import ai.traceable.ratelimiting.config.service.v2.RegionCondition.Region;
import ai.traceable.ratelimiting.config.service.v2.TransactionActionConfig;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DLPBlockingPolicyDataFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private DLPBlockingPolicyDataFetcher rateLimiterTransactionBasedDataFetcher;
  private BlockingRulesSupplier blockingRulesSupplier;
  private static final long activeTimestamp = System.currentTimeMillis() + 10000L;

  @BeforeEach
  void setUp() {
    blockingRulesSupplier = mock(BlockingRulesSupplier.class);

    BlockingRulesUtils blockingRulesUtils = mock(BlockingRulesUtils.class);

    when(blockingRulesUtils.generateBlockingStatus(0, BlockingPolicyData.RuleType.ALLOW))
        .thenReturn(BlockingPolicyData.Status.ALLOWED);
    when(blockingRulesUtils.generateBlockingStatus(0, BlockingPolicyData.RuleType.BLOCK))
        .thenReturn(BlockingPolicyData.Status.DENIED);

    when(blockingRulesUtils.generateBlockingStatus(
            activeTimestamp, BlockingPolicyData.RuleType.ALLOW))
        .thenReturn(BlockingPolicyData.Status.SNOOZED);
    when(blockingRulesUtils.generateBlockingStatus(
            activeTimestamp, BlockingPolicyData.RuleType.BLOCK))
        .thenReturn(BlockingPolicyData.Status.SUSPENDED);

    rateLimiterTransactionBasedDataFetcher = new DLPBlockingPolicyDataFetcher(blockingRulesUtils);
  }

  @Test
  void getRateLimitingRulesTest() {
    LinkedHashMap<String, List<RateLimitingModsecRule>> sampleMap = new LinkedHashMap<>();
    sampleMap.put("service1", List.of(buildSampleRateLimitingModsecRule("1", false)));
    sampleMap.put("service2", List.of(buildSampleRateLimitingModsecRule("2", true)));

    doReturn(sampleMap)
        .when(blockingRulesSupplier)
        .getDlpRules(new LinkedHashSet<>(List.of("service1", "service2")));

    LinkedHashMap<String, List<BlockingPolicyData>> serviceScopeDLPRules =
        rateLimiterTransactionBasedDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder()
                    .environmentId(Optional.of(ENVIRONMENT_ID))
                    .serviceNames(List.of("service1", "service2"))
                    .build(),
                blockingRulesSupplier)
            .getServiceScopedBlockingPolicyMap();

    assertEquals(2, serviceScopeDLPRules.size());

    assertEquals(1, serviceScopeDLPRules.get("service1").size());
    assertEquals(
        buildBlockingDetails("1"),
        serviceScopeDLPRules.get("service1").get(0).getBlockingDetails());
    assertEquals(
        Category.DATA_EXFILTRATION, serviceScopeDLPRules.get("service1").get(0).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.ALLOW,
        serviceScopeDLPRules.get("service1").get(0).getRuleType());
    assertEquals(
        BlockingPolicyData.Status.SNOOZED, serviceScopeDLPRules.get("service1").get(0).getStatus());
    assertEquals(activeTimestamp, serviceScopeDLPRules.get("service1").get(0).getTimestamp());
    assertEquals(
        ExemptionInfoEncoder.getEncodedDLPRuleExemptionInfo(
            "rule-id-1", "rule-name-1", EVENT_SEVERITY_UNSPECIFIED.name()),
        serviceScopeDLPRules.get("service1").get(0).getInfo());

    assertEquals(1, serviceScopeDLPRules.get("service2").size());
    assertEquals(
        buildBlockingDetails("2"),
        serviceScopeDLPRules.get("service2").get(0).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.DATA_EXFILTRATION,
        serviceScopeDLPRules.get("service2").get(0).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.BLOCK,
        serviceScopeDLPRules.get("service2").get(0).getRuleType());
    assertEquals(
        BlockingPolicyData.Status.SUSPENDED,
        serviceScopeDLPRules.get("service2").get(0).getStatus());
    assertEquals(activeTimestamp, serviceScopeDLPRules.get("service2").get(0).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomSignatureRuleViolationInfo(
            "rule-id-2", "rule-name-2", EVENT_SEVERITY_HIGH.name()),
        serviceScopeDLPRules.get("service2").get(0).getInfo());
  }

  private static RateLimitingModsecRule buildSampleRateLimitingModsecRule(
      String id, boolean block) {
    Action action;
    if (block) {
      action =
          Action.newBuilder()
              .setBlock(Block.newBuilder().setEventSeverity(EVENT_SEVERITY_HIGH))
              .build();
    } else {
      action = Action.newBuilder().setAllow(Allow.getDefaultInstance()).build();
    }
    return RateLimitingModsecRule.newBuilder()
        .setId("rule-id-" + id)
        .setData(
            RateLimitingRuleData.newBuilder()
                .setName("rule-name-" + id)
                .setCategory(CATEGORY_DATA_EXFILTRATION)
                .setCondition(
                    Condition.newBuilder()
                        .setCompositeCondition(
                            CompositeCondition.newBuilder()
                                .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                                .addChildren(
                                    Condition.newBuilder()
                                        .setLeafCondition(
                                            LeafCondition.newBuilder()
                                                .setRegionCondition(
                                                    RegionCondition.newBuilder()
                                                        .addRegionIdentifiers(
                                                            Region.newBuilder()
                                                                .setCountryIsoCode("Bhutan")))))
                                .addChildren(
                                    Condition.newBuilder()
                                        .setLeafCondition(
                                            LeafCondition.newBuilder()
                                                .setIpAddressCondition(
                                                    IpAddressCondition.newBuilder()
                                                        .addCidrIpRanges("1.2.3.4/5")
                                                        .addIpAddresses("1.2.3.4"))))
                                .addChildren(
                                    Condition.newBuilder()
                                        .setLeafCondition(
                                            LeafCondition.newBuilder()
                                                .setIpLocationTypeCondition(
                                                    IpLocationTypeCondition.newBuilder()
                                                        .addIpLocationTypes(
                                                            IpLocationType
                                                                .IP_LOCATION_TYPE_BOT))))))
                .setTransactionActionConfig(
                    TransactionActionConfig.newBuilder()
                        .setAction(action)
                        .setExpirationTimestampMillis(activeTimestamp)))
        .addAssociatedModsecRuleIds(
            ModsecRuleIdInfo.newBuilder()
                .addMatchConditions(MatchCondition.newBuilder().setMatchId("rule-id-" + id))
                .setType(IdType.ID_TYPE_KEY_VALUE_CONDITION_URL_REGEXES))
        .addAssociatedModsecRuleIds(
            ModsecRuleIdInfo.newBuilder()
                .addMatchConditions(MatchCondition.newBuilder().setMatchId("credit-card-id"))
                .addMatchConditions(
                    MatchCondition.newBuilder().setMatchId("ssn-id").addIgnoreIds("ignore-id"))
                .setType(IdType.ID_TYPE_DATA_TYPE_CUSTOM_LOCATION))
        .build();
  }

  private static CombinationBlockingDetails buildBlockingDetails(String id) {
    return CombinationBlockingDetails.builder()
        .operator(Operator.AND)
        .blockingDetailsOperands(
            List.of(
                CombinationBlockingDetails.builder()
                    .operator(Operator.AND)
                    .blockingDetailsOperands(
                        List.of(
                            RegionBlockingDetails.builder().region("Bhutan").build(),
                            IpBlockingDetails.builder()
                                .ipRange("1.2.3.4/5")
                                .ipAddress("1.2.3.4")
                                .build(),
                            IpTypeBlockingDetails.builder()
                                .ipType(
                                    ai.traceable.malicioussources.config.service.v1.IpLocationType
                                        .IP_LOCATION_TYPE_BOT)
                                .build()))
                    .build(),
                CustomSignatureBlockingDetails.builder().ruleId("rule-id-" + id).build(),
                CombinationBlockingDetails.builder()
                    .operator(Operator.OR)
                    .blockingDetailsOperands(
                        List.of(
                            CustomSignatureBlockingDetails.builder()
                                .ruleId("credit-card-id")
                                .build(),
                            CombinationBlockingDetails.builder()
                                .operator(Operator.AND)
                                .blockingDetailsOperands(
                                    List.of(
                                        CombinationBlockingDetails.builder()
                                            .operator(Operator.NOT)
                                            .blockingDetailsOperands(
                                                Collections.singleton(
                                                    CustomSignatureBlockingDetails.builder()
                                                        .ruleId("ignore-id")
                                                        .build()))
                                            .build(),
                                        CustomSignatureBlockingDetails.builder()
                                            .ruleId("ssn-id")
                                            .build()))
                                .build()))
                    .build()))
        .build();
  }
}
