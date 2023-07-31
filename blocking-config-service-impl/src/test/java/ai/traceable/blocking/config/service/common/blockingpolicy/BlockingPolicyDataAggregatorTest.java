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

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CustomSignatureBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpTypeBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.ModsecBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.RegionBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
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
  private BlockingPolicyDataFetcherBase actorBasedDataFetcher;
  private BlockingPolicyDataFetcherBase customIpBasedDataFetcher;
  private BlockingPolicyDataFetcherBase customSignatureDataFetcher;
  private BlockingPolicyDataFetcherBase modsecDataFetcher;
  private BlockingPolicyDataFetcherBase regionDataFetcher;
  private BlockingPolicyDataFetcherBase maliciousSourceRuleDataFetcher;
  private Set<BlockingPolicyDataFetcherBase> blockingPolicyDataFetchers;
  private BlockingPolicyDataAggregator orderedBlockingDetailsBase;

  @BeforeEach
  void setUp() {
    this.actorBasedDataFetcher = mock(BlockingPolicyDataFetcherBase.class);
    this.customIpBasedDataFetcher = mock(BlockingPolicyDataFetcherBase.class);
    this.customSignatureDataFetcher = mock(BlockingPolicyDataFetcherBase.class);
    this.modsecDataFetcher = mock(BlockingPolicyDataFetcherBase.class);
    this.regionDataFetcher = mock(BlockingPolicyDataFetcherBase.class);
    this.maliciousSourceRuleDataFetcher = mock(BlockingPolicyDataFetcherBase.class);
    this.blockingPolicyDataFetchers =
        Set.of(
            customSignatureDataFetcher,
            maliciousSourceRuleDataFetcher,
            regionDataFetcher,
            actorBasedDataFetcher,
            customIpBasedDataFetcher,
            modsecDataFetcher);
    orderedBlockingDetailsBase = new BlockingPolicyDataAggregator(this.blockingPolicyDataFetchers);
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
            REQUEST_CONTEXT,
            BlockingPolicyDataFilter.builder().environmentId(Optional.of(ENVIRONMENT_ID)).build());

    assertEquals(desiredPrecedenceOrder.size(), blockingRules.size());
    for (int i = 0; i < desiredPrecedenceOrder.size(); i++) {
      assertEquals(desiredPrecedenceOrder.get(i), blockingRules.get(i).getInfo());
    }
  }

  private void initializeMocks() {
    doReturn(
            List.of(
                BlockingPolicyData.builder()
                    .blockingDetails(
                        IpBlockingDetails.builder().ipAddresses(List.of("1.2.3.4")).build())
                    .category(Category.THREAT_ACTOR)
                    .ruleType(RuleType.BLOCK)
                    .bucket(THREAT_ACTOR_BASED_IP_VIOLATIONS)
                    .info("threat-actor-violation")
                    .build(),
                BlockingPolicyData.builder()
                    .blockingDetails(
                        IpBlockingDetails.builder().ipAddresses(List.of("1.2.3.4")).build())
                    .category(Category.THREAT_ACTOR)
                    .ruleType(RuleType.ALLOW)
                    .bucket(THREAT_ACTOR_BASED_IP_EXEMPTIONS)
                    .info("threat-actor-exemption")
                    .build(),
                BlockingPolicyData.builder()
                    .blockingDetails(
                        IpBlockingDetails.builder().ipAddresses(List.of("1.2.3.4")).build())
                    .category(Category.RATE_LIMIT)
                    .ruleType(RuleType.BLOCK)
                    .bucket(RATE_LIMITING_BASED_IP_VIOLATIONS)
                    .info("rate-limit-violation")
                    .build(),
                BlockingPolicyData.builder()
                    .blockingDetails(
                        IpBlockingDetails.builder().ipAddresses(List.of("1.2.3.4")).build())
                    .category(Category.EMAIL_DOMAIN_RULE)
                    .ruleType(RuleType.ALLOW)
                    .bucket(EMAIL_DOMAIN_BASED_EXEMPTIONS)
                    .info("email-domain-exemption")
                    .build(),
                BlockingPolicyData.builder()
                    .blockingDetails(
                        IpBlockingDetails.builder().ipAddresses(List.of("1.2.3.4")).build())
                    .category(Category.EMAIL_DOMAIN_RULE)
                    .ruleType(RuleType.BLOCK)
                    .bucket(EMAIL_DOMAIN_BASED_VIOLATIONS)
                    .info("email-domain-violation")
                    .build()))
        .when(actorBasedDataFetcher)
        .getBlockingPolicyData(
            REQUEST_CONTEXT,
            BlockingPolicyDataFilter.builder().environmentId(Optional.of(ENVIRONMENT_ID)).build());

    doReturn(
            List.of(
                BlockingPolicyData.builder()
                    .blockingDetails(
                        IpBlockingDetails.builder().ipAddresses(List.of("1.2.3.4")).build())
                    .category(Category.CUSTOM_IP_RULE)
                    .ruleType(RuleType.ALLOW)
                    .bucket(IP_RANGE_EXEMPTIONS)
                    .info("custom-ip-based-exemption")
                    .build()))
        .when(customIpBasedDataFetcher)
        .getBlockingPolicyData(
            REQUEST_CONTEXT,
            BlockingPolicyDataFilter.builder().environmentId(Optional.of(ENVIRONMENT_ID)).build());

    doReturn(
            List.of(
                BlockingPolicyData.builder()
                    .category(Category.CUSTOM_SIGNATURE_RULE)
                    .ruleType(RuleType.BLOCK)
                    .bucket(CUSTOM_SIGNATURE_VIOLATIONS)
                    .info("custom-signature-violation")
                    .blockingDetails(
                        CustomSignatureBlockingDetails.builder()
                            .ruleId("custom-signature-rule-id")
                            .build())
                    .build(),
                BlockingPolicyData.builder()
                    .category(Category.CUSTOM_SIGNATURE_RULE)
                    .ruleType(RuleType.ALLOW)
                    .bucket(CUSTOM_SIGNATURE_EXEMPTIONS)
                    .info("custom-signature-exemption")
                    .blockingDetails(
                        CustomSignatureBlockingDetails.builder()
                            .ruleId("custom-signature-rule-id")
                            .build())
                    .build()))
        .when(customSignatureDataFetcher)
        .getBlockingPolicyData(
            REQUEST_CONTEXT,
            BlockingPolicyDataFilter.builder().environmentId(Optional.of(ENVIRONMENT_ID)).build());

    doReturn(
            List.of(
                BlockingPolicyData.builder()
                    .blockingDetails(
                        ModsecBlockingDetails.builder().ruleId("modsec-rule-id-1").build())
                    .category(Category.MODSECURITY)
                    .ruleType(RuleType.BLOCK)
                    .bucket(MODSEC_VIOLATIONS)
                    .info("modsec-violation")
                    .build()))
        .when(modsecDataFetcher)
        .getBlockingPolicyData(
            REQUEST_CONTEXT,
            BlockingPolicyDataFilter.builder().environmentId(Optional.of(ENVIRONMENT_ID)).build());

    doReturn(
            List.of(
                BlockingPolicyData.builder()
                    .category(Category.CUSTOM_REGION_RULE)
                    .ruleType(RuleType.BLOCK_ALL_EXCEPT)
                    .info("region-violation")
                    .bucket(REGION_VIOLATIONS)
                    .blockingDetails(
                        RegionBlockingDetails.builder().regions(List.of("Bhutan")).build())
                    .build(),
                BlockingPolicyData.builder()
                    .category(Category.CUSTOM_REGION_RULE)
                    .ruleType(RuleType.BLOCK_ALL_EXCEPT)
                    .bucket(REGION_BLOCK_ALL_EXCEPT_VIOLATIONS)
                    .info("region-block-all-except")
                    .blockingDetails(
                        RegionBlockingDetails.builder().regions(List.of("Bhutan")).build())
                    .build()))
        .when(regionDataFetcher)
        .getBlockingPolicyData(
            REQUEST_CONTEXT,
            BlockingPolicyDataFilter.builder().environmentId(Optional.of(ENVIRONMENT_ID)).build());

    when(maliciousSourceRuleDataFetcher.getBlockingPolicyData(
            REQUEST_CONTEXT,
            BlockingPolicyDataFilter.builder().environmentId(Optional.of(ENVIRONMENT_ID)).build()))
        .thenReturn(
            List.of(
                BlockingPolicyData.builder()
                    .blockingDetails(
                        IpTypeBlockingDetails.builder()
                            .ipTypes(List.of(IpLocationType.IP_LOCATION_TYPE_BOT))
                            .build())
                    .category(Category.IP_TYPE_RULE)
                    .ruleType(RuleType.BLOCK)
                    .bucket(IP_TYPE_VIOLATIONS)
                    .info("malicious-source-ip-type-violation")
                    .build(),
                BlockingPolicyData.builder()
                    .blockingDetails(
                        IpBlockingDetails.builder()
                            .ipRanges(List.of("1.2.3.4"))
                            .ipAddresses(List.of("1.2.3.4"))
                            .build())
                    .category(Category.CUSTOM_IP_RULE)
                    .ruleType(RuleType.BLOCK)
                    .bucket(IP_RANGE_VIOLATIONS)
                    .info("malicious-source-ip-range-violation")
                    .build(),
                BlockingPolicyData.builder()
                    .blockingDetails(
                        IpBlockingDetails.builder()
                            .ipRanges(List.of("1.2.3.4"))
                            .ipAddresses(List.of("1.2.3.4"))
                            .build())
                    .category(Category.CUSTOM_IP_RULE)
                    .ruleType(RuleType.BLOCK_ALL_EXCEPT)
                    .bucket(IP_RANGE_BLOCK_ALL_EXCEPT_VIOLATIONS)
                    .info("malicious-source-ip-range-block-all-except")
                    .build()));
  }
}
