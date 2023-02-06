package ai.traceable.blocking.config.service.common.blockingpolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.ActorBasedDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.CustomIpBasedDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.CustomSignatureDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.IpTypeDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.ModsecDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.RegionDataFetcher;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.actor.ActorBasedRulesCollection;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import java.util.ArrayList;
import java.util.EnumMap;
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
  private IpTypeDataFetcher ipTypeDataFetcher;
  private BlockingPolicyDataAggregator orderedBlockingDetailsBase;

  @BeforeEach
  void setUp() {
    this.actorBasedDataFetcher = mock(ActorBasedDataFetcher.class);
    this.customIpBasedDataFetcher = mock(CustomIpBasedDataFetcher.class);
    this.customSignatureDataFetcher = mock(CustomSignatureDataFetcher.class);
    this.modsecDataFetcher = mock(ModsecDataFetcher.class);
    this.regionDataFetcher = mock(RegionDataFetcher.class);
    this.ipTypeDataFetcher = mock(IpTypeDataFetcher.class);
    orderedBlockingDetailsBase =
        new BlockingPolicyDataAggregator(
            actorBasedDataFetcher,
            customIpBasedDataFetcher,
            customSignatureDataFetcher,
            modsecDataFetcher,
            regionDataFetcher,
            ipTypeDataFetcher);
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
                "custom-ip-based-block-all-except",
                "custom-ip-based-violation",
                "threat-actor-violation",
                "email-domain-violation",
                "ip-type-violation",
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

    // TODO test in v1
    //    // Test error handling
    //    doThrow(new RuntimeException())
    //        .when(actorBasedDataFetcher)
    //        .getActorBasedRules(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));
    //    blockingRules =
    //        orderedBlockingDetailsBase.getOrderedBlockingRules(
    //            REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));
    //    assertEquals(0, blockingRules.size());
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

    doReturn(
            List.of(
                BlockingPolicyData.builder()
                    .ipTypes(List.of(IpLocationType.IP_LOCATION_TYPE_BOT))
                    .category(Category.IP_TYPE_RULE)
                    .ruleType(RuleType.BLOCK)
                    .info("ip-type-violation")
                    .build()))
        .when(ipTypeDataFetcher)
        .getIpTypeViolations(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));
  }
}
