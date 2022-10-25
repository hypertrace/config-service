package ai.traceable.blocking.config.service.blockingpolicy;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_IP_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_REGION_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_MODSECURITY;
import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_RATE_LIMIT;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.ActorBasedDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.CustomIpBasedDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.CustomSignatureDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.ModsecDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.RegionDataFetcher;
import ai.traceable.blocking.config.service.blockingpolicy.fetchers.actor.ActorBasedRulesCollection;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingPolicyConfiguration;
import ai.traceable.blocking.config.service.v1.CustomSignatureDetails;
import ai.traceable.blocking.config.service.v1.IpDetails;
import ai.traceable.blocking.config.service.v1.ModsecDetails;
import ai.traceable.blocking.config.service.v1.RegionDetails;
import ai.traceable.config.utils.UuidGenerator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockingPolicyConfigurationManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private final UuidGenerator uuidGenerator = new UuidGenerator();
  private ActorBasedDataFetcher actorBasedDataFetcher;
  private CustomIpBasedDataFetcher customIpBasedDataFetcher;
  private CustomSignatureDataFetcher customSignatureDataFetcher;
  private ModsecDataFetcher modsecDataFetcher;
  private RegionDataFetcher regionDataFetcher;
  private BlockingPolicyConfigurationManager blockingPolicyConfigurationManager;

  @BeforeEach
  void setUp() {
    this.actorBasedDataFetcher = mock(ActorBasedDataFetcher.class);
    this.customIpBasedDataFetcher = mock(CustomIpBasedDataFetcher.class);
    this.customSignatureDataFetcher = mock(CustomSignatureDataFetcher.class);
    this.modsecDataFetcher = mock(ModsecDataFetcher.class);
    this.regionDataFetcher = mock(RegionDataFetcher.class);
    blockingPolicyConfigurationManager =
        new DefaultBlockingPolicyConfigurationManager(
            actorBasedDataFetcher,
            customIpBasedDataFetcher,
            customSignatureDataFetcher,
            modsecDataFetcher,
            regionDataFetcher,
            uuidGenerator);
  }

  @Test
  void getBlockingPolicyConfiguration() {
    // TESTING ORDERING OF RULES, refer to
    // https://traceableai.atlassian.net/wiki/spaces/Engineering/pages/1265139838/Blocking+rules+-+evaluation+order+of+precedence
    ArrayList<String> desiredPrecedenceOrder =
        new ArrayList<>(
            List.of(
                "custom-ip-based-exemption",
                "threat-actor-exemption",
                "custom-signature-exemption",
                "custom-signature-violation",
                "modsec-violation",
                "custom-ip-based-block-all-except",
                "custom-ip-based-violation",
                "threat-actor-violation",
                "region-block-all-except",
                "region-violation",
                "rate-limit-violation"));

    initializeMocks();

    BlockingPolicyConfiguration blockingRules =
        blockingPolicyConfigurationManager.getBlockingPolicyConfiguration(
            REQUEST_CONTEXT, "", Optional.of(ENVIRONMENT_ID));

    assertEquals(desiredPrecedenceOrder.get(0), blockingRules.getBlockingDetailsList(0).getInfo());
    assertEquals(desiredPrecedenceOrder.get(1), blockingRules.getBlockingDetailsList(1).getInfo());
    assertEquals(desiredPrecedenceOrder.get(2), blockingRules.getBlockingDetailsList(2).getInfo());
    assertEquals(desiredPrecedenceOrder.get(3), blockingRules.getBlockingDetailsList(3).getInfo());
    assertEquals(desiredPrecedenceOrder.get(4), blockingRules.getBlockingDetailsList(4).getInfo());
    assertEquals(desiredPrecedenceOrder.get(5), blockingRules.getBlockingDetailsList(5).getInfo());
    assertEquals(desiredPrecedenceOrder.get(6), blockingRules.getBlockingDetailsList(6).getInfo());
    assertEquals(desiredPrecedenceOrder.get(7), blockingRules.getBlockingDetailsList(7).getInfo());
    assertEquals(desiredPrecedenceOrder.get(8), blockingRules.getBlockingDetailsList(8).getInfo());
    assertEquals(desiredPrecedenceOrder.get(9), blockingRules.getBlockingDetailsList(9).getInfo());
    assertEquals(
        desiredPrecedenceOrder.get(10), blockingRules.getBlockingDetailsList(10).getInfo());

    // Test the hash based mechanism
    BlockingPolicyConfiguration blockingRules2 =
        blockingPolicyConfigurationManager.getBlockingPolicyConfiguration(
            REQUEST_CONTEXT, blockingRules.getHash(), Optional.of(ENVIRONMENT_ID));
    assertEquals(blockingRules.getHash(), blockingRules2.getHash());
    assertEquals(0, blockingRules2.getBlockingDetailsListCount());
    BlockingPolicyConfiguration blockingRules3 =
        blockingPolicyConfigurationManager.getBlockingPolicyConfiguration(
            REQUEST_CONTEXT, "", Optional.of(ENVIRONMENT_ID));
    assertNotEquals(0, blockingRules3.getBlockingDetailsListCount());

    // Test error handling
    doThrow(new RuntimeException())
        .when(actorBasedDataFetcher)
        .getActorBasedRules(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));
    BlockingPolicyConfiguration blockingRules4 =
        blockingPolicyConfigurationManager.getBlockingPolicyConfiguration(
            REQUEST_CONTEXT, "random", Optional.of(ENVIRONMENT_ID));
    assertEquals("random", blockingRules4.getHash());
    assertEquals(0, blockingRules4.getBlockingDetailsListCount());
  }

  private void initializeMocks() {
    doReturn(
            new ActorBasedRulesCollection(
                List.of(
                    BlockingDetails.newBuilder()
                        .setIpDetails(IpDetails.newBuilder().addIpAddresses("1.2.3.4").build())
                        .setCategory(BLOCKING_CATEGORY_RATE_LIMIT)
                        .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK)
                        .setInfo("threat-actor-violation")
                        .build()),
                List.of(
                    BlockingDetails.newBuilder()
                        .setIpDetails(IpDetails.newBuilder().addIpAddresses("1.2.3.4").build())
                        .setCategory(BLOCKING_CATEGORY_RATE_LIMIT)
                        .setBlockingRuleType(BLOCKING_RULE_TYPE_ALLOW)
                        .setInfo("threat-actor-exemption")
                        .build()),
                List.of(
                    BlockingDetails.newBuilder()
                        .setIpDetails(IpDetails.newBuilder().addIpAddresses("1.2.3.4").build())
                        .setCategory(BLOCKING_CATEGORY_RATE_LIMIT)
                        .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK)
                        .setInfo("rate-limit-violation")
                        .build())))
        .when(actorBasedDataFetcher)
        .getActorBasedRules(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    doReturn(
            Map.of(
                BLOCKING_RULE_TYPE_BLOCK,
                List.of(
                    BlockingDetails.newBuilder()
                        .setIpDetails(IpDetails.newBuilder().addIpAddresses("1.2.3.4").build())
                        .setCategory(BLOCKING_CATEGORY_CUSTOM_IP_RULE)
                        .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK)
                        .setInfo("custom-ip-based-violation")
                        .build()),
                BLOCKING_RULE_TYPE_ALLOW,
                List.of(
                    BlockingDetails.newBuilder()
                        .setIpDetails(IpDetails.newBuilder().addIpAddresses("1.2.3.4").build())
                        .setCategory(BLOCKING_CATEGORY_CUSTOM_IP_RULE)
                        .setBlockingRuleType(BLOCKING_RULE_TYPE_ALLOW)
                        .setInfo("custom-ip-based-exemption")
                        .build()),
                BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT,
                List.of(
                    BlockingDetails.newBuilder()
                        .setIpDetails(IpDetails.newBuilder().addIpAddresses("1.2.3.4").build())
                        .setCategory(BLOCKING_CATEGORY_CUSTOM_IP_RULE)
                        .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT)
                        .setInfo("custom-ip-based-block-all-except")
                        .build())))
        .when(customIpBasedDataFetcher)
        .getCustomIpBasedRules(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    doReturn(
            Map.of(
                BLOCKING_RULE_TYPE_BLOCK,
                List.of(
                    BlockingDetails.newBuilder()
                        .setCategory(BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE)
                        .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK)
                        .setInfo("custom-signature-violation")
                        .setCustomSignatureDetails(
                            CustomSignatureDetails.newBuilder()
                                .setRuleId("custom-signature-rule-id")
                                .build())
                        .build()),
                BLOCKING_RULE_TYPE_ALLOW,
                List.of(
                    BlockingDetails.newBuilder()
                        .setCategory(BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE)
                        .setBlockingRuleType(BLOCKING_RULE_TYPE_ALLOW)
                        .setInfo("custom-signature-exemption")
                        .setCustomSignatureDetails(
                            CustomSignatureDetails.newBuilder()
                                .setRuleId("custom-signature-rule-id")
                                .build())
                        .build())))
        .when(customSignatureDataFetcher)
        .getCustomSignatureRules(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    doReturn(
            List.of(
                BlockingDetails.newBuilder()
                    .setModsecDetails(
                        ModsecDetails.newBuilder().setRuleId("modsec-rule-id-1").build())
                    .setCategory(BLOCKING_CATEGORY_MODSECURITY)
                    .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK)
                    .setInfo("modsec-violation")
                    .build()))
        .when(modsecDataFetcher)
        .getModsecViolations(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));

    doReturn(
            Map.of(
                BLOCKING_RULE_TYPE_BLOCK,
                List.of(
                    BlockingDetails.newBuilder()
                        .setCategory(BLOCKING_CATEGORY_CUSTOM_REGION_RULE)
                        .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT)
                        .setInfo("region-violation")
                        .setRegionDetails(RegionDetails.newBuilder().addRegions("Bhutan").build())
                        .build()),
                BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT,
                List.of(
                    BlockingDetails.newBuilder()
                        .setCategory(BLOCKING_CATEGORY_CUSTOM_REGION_RULE)
                        .setBlockingRuleType(BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT)
                        .setInfo("region-block-all-except")
                        .setRegionDetails(RegionDetails.newBuilder().addRegions("Bhutan").build())
                        .build())))
        .when(regionDataFetcher)
        .getRegionBasedRules(REQUEST_CONTEXT, Optional.of(ENVIRONMENT_ID));
  }
}
