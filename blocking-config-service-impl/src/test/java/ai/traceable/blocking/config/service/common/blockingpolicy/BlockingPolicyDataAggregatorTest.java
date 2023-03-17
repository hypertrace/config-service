package ai.traceable.blocking.config.service.common.blockingpolicy;

import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.CUSTOM_SIGNATURE_EXEMPTIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.CUSTOM_SIGNATURE_VIOLATIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.EMAIL_DOMAIN_BASED_EXEMPTIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.EMAIL_DOMAIN_BASED_VIOLATIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.IP_RANGE_BLOCK_ALL_EXCEPT_VIOLATIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.IP_RANGE_EXEMPTIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.IP_RANGE_VIOLATIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.IP_TYPE_VIOLATIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.MODSEC_VIOLATIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.RATE_LIMITING_BASED_IP_VIOLATIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.REGION_BLOCK_ALL_EXCEPT_VIOLATIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.REGION_VIOLATIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.THREAT_ACTOR_BASED_IP_EXEMPTIONS;
import static ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket.THREAT_ACTOR_BASED_IP_VIOLATIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.ActorBasedDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.CustomIpBasedDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.CustomSignatureDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.DataFetcherBase;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.MaliciousSourcesDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.ModsecDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.RegionDataFetcher;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
  private Set<DataFetcherBase> dataFetcherBases;

  @BeforeEach
  void setUp() {
    this.actorBasedDataFetcher = mock(ActorBasedDataFetcher.class);
    this.customIpBasedDataFetcher = mock(CustomIpBasedDataFetcher.class);
    this.customSignatureDataFetcher = mock(CustomSignatureDataFetcher.class);
    this.modsecDataFetcher = mock(ModsecDataFetcher.class);
    this.regionDataFetcher = mock(RegionDataFetcher.class);
    this.maliciousSourceRuleDataFetcher = mock(MaliciousSourcesDataFetcher.class);
    this.dataFetcherBases =
        Set.of(
            customSignatureDataFetcher,
            maliciousSourceRuleDataFetcher,
            regionDataFetcher,
            actorBasedDataFetcher,
            customIpBasedDataFetcher,
            modsecDataFetcher);
    orderedBlockingDetailsBase = new BlockingPolicyDataAggregator(this.dataFetcherBases);
  }

  @Test
  void getOrderedBlockingRules() {
    // TESTING ORDERING OF RULES, refer to
    // https://traceableai.atlassian.net/wiki/spaces/Engineering/pages/1265139838/Blocking+rules+-+evaluation+order+of+precedence
    ArrayList<String> desiredPrecedenceOrder =
        new ArrayList<>(
            List.of(
                "custom-ip-based-exemption",
                "threat-actor-exemption",
                "email-domain-exemption",
                "custom-signature-exemption",
                "custom-signature-violation",
                "modsec-violation",
                "malicious-source-ip-range-block-all-except",
                "malicious-source-ip-range-violation",
                "threat-actor-violation",
                "email-domain-violation",
                "malicious-source-ip-type-violation",
                "region-block-all-except",
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
            List.of(
                BlockingPolicyData.builder()
                    .ipAddresses(List.of("1.2.3.4"))
                    .category(Category.THREAT_ACTOR)
                    .ruleType(RuleType.BLOCK)
                    .bucket(THREAT_ACTOR_BASED_IP_VIOLATIONS)
                    .info("threat-actor-violation")
                    .build(),
                BlockingPolicyData.builder()
                    .ipAddresses(List.of("1.2.3.4"))
                    .category(Category.THREAT_ACTOR)
                    .ruleType(RuleType.ALLOW)
                    .bucket(THREAT_ACTOR_BASED_IP_EXEMPTIONS)
                    .info("threat-actor-exemption")
                    .build(),
                BlockingPolicyData.builder()
                    .ipAddresses(List.of("1.2.3.4"))
                    .category(Category.RATE_LIMIT)
                    .ruleType(RuleType.BLOCK)
                    .bucket(RATE_LIMITING_BASED_IP_VIOLATIONS)
                    .info("rate-limit-violation")
                    .build(),
                BlockingPolicyData.builder()
                    .ipAddresses(List.of("1.2.3.4"))
                    .category(Category.EMAIL_DOMAIN_RULE)
                    .ruleType(RuleType.ALLOW)
                    .bucket(EMAIL_DOMAIN_BASED_EXEMPTIONS)
                    .info("email-domain-exemption")
                    .build(),
                BlockingPolicyData.builder()
                    .ipAddresses(List.of("1.2.3.4"))
                    .category(Category.EMAIL_DOMAIN_RULE)
                    .ruleType(RuleType.BLOCK)
                    .bucket(EMAIL_DOMAIN_BASED_VIOLATIONS)
                    .info("email-domain-violation")
                    .build()))
        .when(actorBasedDataFetcher)
        .getBlockingPolicyData(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    doReturn(
            List.of(
                BlockingPolicyData.builder()
                    .ipAddresses(List.of("1.2.3.4"))
                    .category(Category.CUSTOM_IP_RULE)
                    .ruleType(RuleType.ALLOW)
                    .bucket(IP_RANGE_EXEMPTIONS)
                    .info("custom-ip-based-exemption")
                    .build()))
        .when(customIpBasedDataFetcher)
        .getBlockingPolicyData(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    doReturn(
            List.of(
                BlockingPolicyData.builder()
                    .category(Category.CUSTOM_SIGNATURE_RULE)
                    .ruleType(RuleType.BLOCK)
                    .bucket(CUSTOM_SIGNATURE_VIOLATIONS)
                    .info("custom-signature-violation")
                    .ruleId("custom-signature-rule-id")
                    .build(),
                BlockingPolicyData.builder()
                    .category(Category.CUSTOM_SIGNATURE_RULE)
                    .ruleType(RuleType.ALLOW)
                    .bucket(CUSTOM_SIGNATURE_EXEMPTIONS)
                    .info("custom-signature-exemption")
                    .ruleId("custom-signature-rule-id")
                    .build()))
        .when(customSignatureDataFetcher)
        .getBlockingPolicyData(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    doReturn(
            List.of(
                BlockingPolicyData.builder()
                    .ruleId("modsec-rule-id-1")
                    .category(Category.MODSECURITY)
                    .ruleType(RuleType.BLOCK)
                    .bucket(MODSEC_VIOLATIONS)
                    .info("modsec-violation")
                    .build()))
        .when(modsecDataFetcher)
        .getBlockingPolicyData(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    doReturn(
            List.of(
                BlockingPolicyData.builder()
                    .category(Category.CUSTOM_REGION_RULE)
                    .ruleType(RuleType.BLOCK_ALL_EXCEPT)
                    .info("region-violation")
                    .bucket(REGION_VIOLATIONS)
                    .regions(List.of("Bhutan"))
                    .build(),
                BlockingPolicyData.builder()
                    .category(Category.CUSTOM_REGION_RULE)
                    .ruleType(RuleType.BLOCK_ALL_EXCEPT)
                    .bucket(REGION_BLOCK_ALL_EXCEPT_VIOLATIONS)
                    .info("region-block-all-except")
                    .regions(List.of("Bhutan"))
                    .build()))
        .when(regionDataFetcher)
        .getBlockingPolicyData(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    when(maliciousSourceRuleDataFetcher.getBlockingPolicyData(
            REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID)))
        .thenReturn(
            List.of(
                BlockingPolicyData.builder()
                    .ipTypes(List.of(IpLocationType.IP_LOCATION_TYPE_BOT))
                    .category(Category.IP_TYPE_RULE)
                    .ruleType(RuleType.BLOCK)
                    .bucket(IP_TYPE_VIOLATIONS)
                    .info("malicious-source-ip-type-violation")
                    .build(),
                BlockingPolicyData.builder()
                    .ipRanges(List.of("1.2.3.4"))
                    .ipAddresses(List.of("1.2.3.4"))
                    .category(Category.CUSTOM_IP_RULE)
                    .ruleType(RuleType.BLOCK)
                    .bucket(IP_RANGE_VIOLATIONS)
                    .info("malicious-source-ip-range-violation")
                    .build(),
                BlockingPolicyData.builder()
                    .ipRanges(List.of("1.2.3.4"))
                    .ipAddresses(List.of("1.2.3.4"))
                    .category(Category.CUSTOM_IP_RULE)
                    .ruleType(RuleType.BLOCK_ALL_EXCEPT)
                    .bucket(IP_RANGE_BLOCK_ALL_EXCEPT_VIOLATIONS)
                    .info("malicious-source-ip-range-block-all-except")
                    .build()));
  }
}
