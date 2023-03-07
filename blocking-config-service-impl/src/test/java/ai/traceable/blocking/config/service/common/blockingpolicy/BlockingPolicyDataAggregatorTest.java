package ai.traceable.blocking.config.service.common.blockingpolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.ActorBasedDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.CustomIpBasedDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.CustomSignatureDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.ModsecDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.RegionDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.ActorBasedRulesCollection;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.MaliciousSourcesDataFetcher;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockingPolicyDataAggregatorTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private ActorBasedDataFetcher actorBasedDataFetcher;
  private CustomIpBasedDataFetcher customIpBasedDataFetcher;
  private CustomSignatureDataFetcher customSignatureDataFetcher;
  private ModsecDataFetcher modsecDataFetcher;
  private RegionDataFetcher regionDataFetcher;
  private BlockingPolicyDataAggregator orderedBlockingDetailsBase;
  private MaliciousSourcesDataFetcher maliciousSourceRuleDataFetcher;

  @BeforeEach
  void setUp() {
    this.actorBasedDataFetcher = mock(ActorBasedDataFetcher.class);
    this.customIpBasedDataFetcher = mock(CustomIpBasedDataFetcher.class);
    this.customSignatureDataFetcher = mock(CustomSignatureDataFetcher.class);
    this.modsecDataFetcher = mock(ModsecDataFetcher.class);
    this.regionDataFetcher = mock(RegionDataFetcher.class);
    this.maliciousSourceRuleDataFetcher = mock(MaliciousSourcesDataFetcher.class);
    orderedBlockingDetailsBase =
        new BlockingPolicyDataAggregator(
            actorBasedDataFetcher,
            customIpBasedDataFetcher,
            customSignatureDataFetcher,
            modsecDataFetcher,
            regionDataFetcher,
            maliciousSourceRuleDataFetcher);
  }

  @Test
  void getOrderedBlockingRules() {
    // TESTING ORDERING OF RULES, refer to
    // https://traceableai.atlassian.net/wiki/spaces/Engineering/pages/1265139838/Blocking+rules+-+evaluation+order+of+precedence
    ArrayList<String> desiredPrecedenceOrder =
        new ArrayList<>(
            List.of(
                "malicious-source-ip-range-exemption",
                "custom-ip-based-exemption",
                "threat-actor-exemption",
                "email-domain-exemption",
                "custom-signature-exemption",
                "custom-signature-violation",
                "modsec-violation",
                "malicious-source-ip-range-block-all-except",
                "custom-ip-based-block-all-except",
                "malicious-source-ip-range-violation",
                "custom-ip-based-violation",
                "threat-actor-violation",
                "email-domain-violation",
                "malicious-source-ip-type-violation",
                "malicious-source-region-block-all-except",
                "region-block-all-except",
                "malicious-source-region-violation",
                "region-violation",
                "rate-limit-violation"));

    initializeMocks();

    List<BlockingPolicyData> blockingRules =
        orderedBlockingDetailsBase.getOrderedBlockingRules(
            REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    assertEquals(desiredPrecedenceOrder.size(), blockingRules.size());
    for (int i = 0; i < desiredPrecedenceOrder.size(); i++) {
      assertEquals(desiredPrecedenceOrder.get(i), blockingRules.get(i).getInfo());
    }
  }

  private void initializeMocks() {
    doReturn(
            new ActorBasedRulesCollection(
                List.of(
                    BlockingPolicyData.builder()
                        .ipAddresses(List.of("1.2.3.4"))
                        .category(Category.RATE_LIMIT)
                        .ruleType(RuleType.BLOCK)
                        .info("threat-actor-violation")
                        .build()),
                List.of(
                    BlockingPolicyData.builder()
                        .ipAddresses(List.of("1.2.3.4"))
                        .category(Category.RATE_LIMIT)
                        .ruleType(RuleType.ALLOW)
                        .info("threat-actor-exemption")
                        .build()),
                List.of(
                    BlockingPolicyData.builder()
                        .ipAddresses(List.of("1.2.3.4"))
                        .category(Category.RATE_LIMIT)
                        .ruleType(RuleType.BLOCK)
                        .info("rate-limit-violation")
                        .build()),
                List.of(
                    BlockingPolicyData.builder()
                        .ipAddresses(List.of("1.2.3.4"))
                        .category(Category.EMAIL_DOMAIN_RULE)
                        .ruleType(RuleType.ALLOW)
                        .info("email-domain-exemption")
                        .build()),
                List.of(
                    BlockingPolicyData.builder()
                        .ipAddresses(List.of("1.2.3.4"))
                        .category(Category.EMAIL_DOMAIN_RULE)
                        .ruleType(RuleType.BLOCK)
                        .info("email-domain-violation")
                        .build())))
        .when(actorBasedDataFetcher)
        .getActorBasedRules(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    doReturn(
            new EnumMap<>(
                Map.of(
                    RuleType.BLOCK,
                    List.of(
                        BlockingPolicyData.builder()
                            .ipAddresses(List.of("1.2.3.4"))
                            .category(Category.CUSTOM_IP_RULE)
                            .ruleType(RuleType.BLOCK)
                            .info("custom-ip-based-violation")
                            .build()),
                    RuleType.ALLOW,
                    List.of(
                        BlockingPolicyData.builder()
                            .ipAddresses(List.of("1.2.3.4"))
                            .category(Category.CUSTOM_IP_RULE)
                            .ruleType(RuleType.ALLOW)
                            .info("custom-ip-based-exemption")
                            .build()),
                    RuleType.BLOCK_ALL_EXCEPT,
                    List.of(
                        BlockingPolicyData.builder()
                            .ipAddresses(List.of("1.2.3.4"))
                            .category(Category.CUSTOM_IP_RULE)
                            .ruleType(RuleType.BLOCK_ALL_EXCEPT)
                            .info("custom-ip-based-block-all-except")
                            .build()))))
        .when(customIpBasedDataFetcher)
        .getCustomIpBasedRules(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    doReturn(
            new EnumMap<>(
                Map.of(
                    RuleType.BLOCK,
                    List.of(
                        BlockingPolicyData.builder()
                            .category(Category.CUSTOM_SIGNATURE_RULE)
                            .ruleType(RuleType.BLOCK)
                            .info("custom-signature-violation")
                            .ruleId("custom-signature-rule-id")
                            .build()),
                    RuleType.ALLOW,
                    List.of(
                        BlockingPolicyData.builder()
                            .category(Category.CUSTOM_SIGNATURE_RULE)
                            .ruleType(RuleType.ALLOW)
                            .info("custom-signature-exemption")
                            .ruleId("custom-signature-rule-id")
                            .build()))))
        .when(customSignatureDataFetcher)
        .getCustomSignatureRules(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    doReturn(
            List.of(
                BlockingPolicyData.builder()
                    .ruleId("modsec-rule-id-1")
                    .category(Category.MODSECURITY)
                    .ruleType(RuleType.BLOCK)
                    .info("modsec-violation")
                    .build()))
        .when(modsecDataFetcher)
        .getModsecViolations(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    doReturn(
            new EnumMap<>(
                Map.of(
                    RuleType.BLOCK,
                    List.of(
                        BlockingPolicyData.builder()
                            .category(Category.CUSTOM_REGION_RULE)
                            .ruleType(RuleType.BLOCK_ALL_EXCEPT)
                            .info("region-violation")
                            .regions(List.of("Bhutan"))
                            .build()),
                    RuleType.BLOCK_ALL_EXCEPT,
                    List.of(
                        BlockingPolicyData.builder()
                            .category(Category.CUSTOM_REGION_RULE)
                            .ruleType(RuleType.BLOCK_ALL_EXCEPT)
                            .info("region-block-all-except")
                            .regions(List.of("Bhutan"))
                            .build()))))
        .when(regionDataFetcher)
        .getRegionBasedRules(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    Map<BlockingPolicyDataBucket, List<BlockingPolicyData>> blockingPolicyDetails = new HashMap<>();

    blockingPolicyDetails.put(
        BlockingPolicyDataBucket.IP_TYPE_VIOLATIONS,
        List.of(
            BlockingPolicyData.builder()
                .ipTypes(List.of(IpLocationType.IP_LOCATION_TYPE_BOT))
                .category(Category.IP_TYPE_RULE)
                .ruleType(RuleType.BLOCK)
                .info("malicious-source-ip-type-violation")
                .build()));
    blockingPolicyDetails.put(
        BlockingPolicyDataBucket.IP_RANGE_VIOLATIONS,
        List.of(
            BlockingPolicyData.builder()
                .ipRanges(List.of("1.2.3.4"))
                .ipAddresses(List.of("1.2.3.4"))
                .category(Category.CUSTOM_IP_RULE)
                .ruleType(RuleType.BLOCK)
                .info("malicious-source-ip-range-violation")
                .build()));
    blockingPolicyDetails.put(
        BlockingPolicyDataBucket.IP_RANGE_EXEMPTIONS,
        List.of(
            BlockingPolicyData.builder()
                .ipRanges(List.of("1.2.3.4"))
                .ipAddresses(List.of("1.2.3.4"))
                .category(Category.CUSTOM_IP_RULE)
                .ruleType(RuleType.ALLOW)
                .info("malicious-source-ip-range-exemption")
                .build()));
    blockingPolicyDetails.put(
        BlockingPolicyDataBucket.IP_RANGE_BLOCK_ALL_EXCEPT_VIOLATIONS,
        List.of(
            BlockingPolicyData.builder()
                .ipRanges(List.of("1.2.3.4"))
                .ipAddresses(List.of("1.2.3.4"))
                .category(Category.CUSTOM_IP_RULE)
                .ruleType(RuleType.BLOCK_ALL_EXCEPT)
                .info("malicious-source-ip-range-block-all-except")
                .build()));
    blockingPolicyDetails.put(
        BlockingPolicyDataBucket.REGION_VIOLATIONS,
        List.of(
            BlockingPolicyData.builder()
                .regions(List.of("afghanistan"))
                .category(Category.CUSTOM_REGION_RULE)
                .ruleType(RuleType.BLOCK)
                .info("malicious-source-region-violation")
                .build()));
    blockingPolicyDetails.put(
        BlockingPolicyDataBucket.REGION_BLOCK_ALL_EXCEPT_VIOLATIONS,
        List.of(
            BlockingPolicyData.builder()
                .regions(List.of("afghanistan"))
                .category(Category.CUSTOM_REGION_RULE)
                .ruleType(RuleType.BLOCK_ALL_EXCEPT)
                .info("malicious-source-region-block-all-except")
                .build()));
    when(maliciousSourceRuleDataFetcher.getMaliciousSourceRuleBlockingDetails(
            REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID)))
        .thenReturn(blockingPolicyDetails);
  }
}
