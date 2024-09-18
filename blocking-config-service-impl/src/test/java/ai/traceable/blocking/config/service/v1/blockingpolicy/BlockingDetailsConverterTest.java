package ai.traceable.blocking.config.service.v1.blockingpolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.ActorBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.Status;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CombinationBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CombinationBlockingDetails.Operator;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CustomSignatureBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpTypeBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.ModsecBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.RegionBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.v1.ActorDetails;
import ai.traceable.blocking.config.service.v1.BlockingCategory;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingDetails.Builder;
import ai.traceable.blocking.config.service.v1.BlockingRuleType;
import ai.traceable.blocking.config.service.v1.BlockingStatus;
import ai.traceable.blocking.config.service.v1.CustomSignatureDetails;
import ai.traceable.blocking.config.service.v1.IpDetails;
import ai.traceable.blocking.config.service.v1.IpType;
import ai.traceable.blocking.config.service.v1.IpTypeDetails;
import ai.traceable.blocking.config.service.v1.ModsecDetails;
import ai.traceable.blocking.config.service.v1.RegionDetails;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class BlockingDetailsConverterTest {
  private static final BlockingDetailsConverter converter =
      new BlockingDetailsConverter(new BlockingDetailsVisitorImpl());
  private static final BlockingPolicyDataFilter filter = mock(BlockingPolicyDataFilter.class);

  @Test
  void testIpRangeConversion() {
    Builder detailsBuilder =
        setBlockingDetails(
            BlockingStatus.BLOCKING_STATUS_ALLOWED,
            "ip-range",
            BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW,
            Optional.of(100L),
            BlockingCategory.BLOCKING_CATEGORY_CUSTOM_IP_RULE);
    detailsBuilder.setIpDetails(
        IpDetails.newBuilder()
            .addAllIpAddresses(List.of("1.2.2.3", "1.2.3.4"))
            .addAllIpRanges(List.of("11.22.33.44/5", "1.2.3.4/5"))
            .build());

    assertEquals(
        detailsBuilder.build(),
        converter.convert(
            setBlockingDetailsInfo(
                Status.ALLOWED,
                "ip-range",
                RuleType.ALLOW,
                100L,
                Category.CUSTOM_IP_RULE,
                IpBlockingDetails.builder()
                    .ipRanges(List.of("11.22.33.44/5", "1.2.3.4/5"))
                    .ipAddresses(List.of("1.2.2.3", "1.2.3.4"))
                    .build()),
            filter));
  }

  @Test
  void testIpTypeConversion() {
    Builder detailsBuilder =
        setBlockingDetails(
            BlockingStatus.BLOCKING_STATUS_DENIED,
            "ip-type",
            BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK,
            Optional.empty(),
            BlockingCategory.BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE);
    detailsBuilder.setIpTypeDetails(
        IpTypeDetails.newBuilder()
            .addAllIpTypes(List.of(IpType.IP_TYPE_BOT, IpType.IP_TYPE_TOR))
            .build());

    assertEquals(
        detailsBuilder.build(),
        converter.convert(
            setBlockingDetailsInfo(
                Status.DENIED,
                "ip-type",
                RuleType.BLOCK,
                0L,
                Category.IP_TYPE_RULE,
                IpTypeBlockingDetails.builder()
                    .ipTypes(
                        List.of(
                            IpLocationType.IP_LOCATION_TYPE_BOT,
                            IpLocationType.IP_LOCATION_TYPE_TOR_EXIT_NODE))
                    .build()),
            filter));
  }

  @Test
  void testRegionConversion() {
    Builder detailsBuilder =
        setBlockingDetails(
            BlockingStatus.BLOCKING_STATUS_SUSPENDED,
            "region-type",
            BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT,
            Optional.of(100L),
            BlockingCategory.BLOCKING_CATEGORY_CUSTOM_REGION_RULE);
    detailsBuilder.setRegionDetails(
        RegionDetails.newBuilder().addAllRegions(List.of("Spain", "Morocco")).build());

    assertEquals(
        detailsBuilder.build(),
        converter.convert(
            setBlockingDetailsInfo(
                Status.SUSPENDED,
                "region-type",
                RuleType.BLOCK_ALL_EXCEPT,
                100L,
                Category.CUSTOM_REGION_RULE,
                RegionBlockingDetails.builder().regions(List.of("Spain", "Morocco")).build()),
            filter));
  }

  @Test
  void testModsecConversion() {
    Builder detailsBuilder =
        setBlockingDetails(
            BlockingStatus.BLOCKING_STATUS_SUSPENDED,
            "modsec-type",
            BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK,
            Optional.empty(),
            BlockingCategory.BLOCKING_CATEGORY_MODSECURITY);
    detailsBuilder.setModsecDetails(ModsecDetails.newBuilder().setRuleId("crs_123").build());

    assertEquals(
        detailsBuilder.build(),
        converter.convert(
            setBlockingDetailsInfo(
                Status.SUSPENDED,
                "modsec-type",
                RuleType.BLOCK,
                0L,
                Category.MODSECURITY,
                ModsecBlockingDetails.builder().ruleId("crs_123").build()),
            filter));
  }

  @Test
  void testCustomSignatureConversion() {
    Builder detailsBuilder =
        setBlockingDetails(
            BlockingStatus.BLOCKING_STATUS_SNOOZED,
            "custom-signature-type",
            BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW,
            Optional.of(100L),
            BlockingCategory.BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE);
    detailsBuilder.setCustomSignatureDetails(
        CustomSignatureDetails.newBuilder().setRuleId("cs-1").build());

    assertEquals(
        detailsBuilder.build(),
        converter.convert(
            setBlockingDetailsInfo(
                Status.SNOOZED,
                "custom-signature-type",
                RuleType.ALLOW,
                100L,
                Category.CUSTOM_SIGNATURE_RULE,
                CustomSignatureBlockingDetails.builder().ruleId("cs-1").build()),
            filter));
  }

  @Test
  void testThreatActorConversion() {
    Builder detailsBuilder =
        setBlockingDetails(
            BlockingStatus.BLOCKING_STATUS_SUSPENDED,
            "threat-actor-type",
            BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK,
            Optional.of(100L),
            BlockingCategory.BLOCKING_CATEGORY_THREAT_ACTOR);
    detailsBuilder.setActorDetails(
        ActorDetails.newBuilder()
            .setUserId("user-1")
            .addAllIpAddresses(List.of("1.2.2.3", "1.2.3.4"))
            .build());
    detailsBuilder.setIpDetails(
        IpDetails.newBuilder().addAllIpAddresses(List.of("1.2.2.3", "1.2.3.4")).build());

    assertEquals(
        detailsBuilder.build(),
        converter.convert(
            setBlockingDetailsInfo(
                Status.SUSPENDED,
                "threat-actor-type",
                RuleType.BLOCK,
                100L,
                Category.THREAT_ACTOR,
                ActorBlockingDetails.builder()
                    .userId("user-1")
                    .ipAddresses(List.of("1.2.2.3", "1.2.3.4"))
                    .build()),
            filter));
  }

  @Test
  void testRateLimitConversion() {
    Builder detailsBuilder =
        setBlockingDetails(
            BlockingStatus.BLOCKING_STATUS_SUSPENDED,
            "rate-limit-type",
            BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK,
            Optional.of(100L),
            BlockingCategory.BLOCKING_CATEGORY_RATE_LIMIT);
    detailsBuilder.setIpDetails(
        IpDetails.newBuilder().addAllIpAddresses(List.of("1.2.2.3", "1.2.3.4")).build());

    assertEquals(
        detailsBuilder.build(),
        converter.convert(
            setBlockingDetailsInfo(
                Status.SUSPENDED,
                "rate-limit-type",
                RuleType.BLOCK,
                100L,
                Category.RATE_LIMIT,
                ActorBlockingDetails.builder()
                    .ipAddresses(List.of("1.2.2.3", "1.2.3.4"))
                    .userId("user-1")
                    .build()),
            filter));
  }

  @Test
  void testDataExfilConversion() {
    Builder detailsBuilder =
        setBlockingDetails(
            BlockingStatus.BLOCKING_STATUS_DENIED,
            "data-exfil-type",
            BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK,
            Optional.of(100L),
            BlockingCategory.BLOCKING_CATEGORY_DATA_EXFILTRATION);
    detailsBuilder.setActorDetails(
        ActorDetails.newBuilder()
            .setUserId("user-1")
            .addAllIpAddresses(List.of("1.2.2.3", "1.2.3.4"))
            .build());
    BlockingDetails actorDetails = detailsBuilder.build();
    detailsBuilder.clearActorDetails();
    detailsBuilder.setIpDetails(
        IpDetails.newBuilder().addAllIpAddresses(List.of("1.2.2.3", "1.2.3.4")).build());

    assertEquals(
        detailsBuilder.build(),
        converter.convert(
            setBlockingDetailsInfo(
                Status.DENIED,
                "data-exfil-type",
                RuleType.BLOCK,
                100L,
                Category.DATA_EXFILTRATION,
                ActorBlockingDetails.builder()
                    .ipAddresses(List.of("1.2.2.3", "1.2.3.4"))
                    .userId("user-1")
                    .build()),
            filter));
  }

  @Test
  void testEnumerationConversion() {
    Builder detailsBuilder =
        setBlockingDetails(
            BlockingStatus.BLOCKING_STATUS_SUSPENDED,
            "enumeration-type",
            BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK,
            Optional.of(100L),
            BlockingCategory.BLOCKING_CATEGORY_ENUMERATION);
    detailsBuilder.setActorDetails(
        ActorDetails.newBuilder()
            .setUserId("user-1")
            .addAllIpAddresses(List.of("1.2.2.3", "1.2.3.4"))
            .build());
    BlockingDetails actorDetails = detailsBuilder.build();
    detailsBuilder.clearActorDetails();
    detailsBuilder.setIpDetails(
        IpDetails.newBuilder().addAllIpAddresses(List.of("1.2.2.3", "1.2.3.4")).build());

    assertEquals(
        detailsBuilder.build(),
        converter.convert(
            setBlockingDetailsInfo(
                Status.SUSPENDED,
                "enumeration-type",
                RuleType.BLOCK,
                100L,
                Category.ENUMERATION,
                ActorBlockingDetails.builder()
                    .ipAddresses(List.of("1.2.2.3", "1.2.3.4"))
                    .userId("user-1")
                    .build()),
            filter));
  }

  @Test
  void testEmailDomainConversion() {
    Builder detailsBuilder =
        setBlockingDetails(
            BlockingStatus.BLOCKING_STATUS_SUSPENDED,
            "email-domain-type",
            BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK,
            Optional.of(100L),
            BlockingCategory.BLOCKING_CATEGORY_MALICIOUS_SOURCES_RULE);
    detailsBuilder.setActorDetails(
        ActorDetails.newBuilder()
            .setUserId("user-1")
            .addAllIpAddresses(List.of("1.2.2.3", "1.2.3.4"))
            .build());
    detailsBuilder.setIpDetails(
        IpDetails.newBuilder().addAllIpAddresses(List.of("1.2.2.3", "1.2.3.4")).build());

    assertEquals(
        detailsBuilder.build(),
        converter.convert(
            setBlockingDetailsInfo(
                Status.SUSPENDED,
                "email-domain-type",
                RuleType.BLOCK,
                100L,
                Category.EMAIL_DOMAIN_RULE,
                ActorBlockingDetails.builder()
                    .ipAddresses(List.of("1.2.2.3", "1.2.3.4"))
                    .userId("user-1")
                    .build()),
            filter));
  }

  @Test
  void testComplexBlockingConversion() {
    assertNull(
        converter.convert(
            setBlockingDetailsInfo(
                Status.SUSPENDED,
                "transaction-based-blocking",
                RuleType.BLOCK,
                100L,
                Category.EMAIL_DOMAIN_RULE,
                CombinationBlockingDetails.builder()
                    .operator(Operator.AND)
                    .blockingDetailsOperands(
                        List.of(
                            IpBlockingDetails.builder().ipAddresses(List.of("1.2.3.4")).build(),
                            RegionBlockingDetails.builder().regions(List.of("Bhutan")).build()))
                    .build()),
            filter));
  }

  private static BlockingPolicyData setBlockingDetailsInfo(
      Status status,
      String info,
      RuleType ruleType,
      Long timestamp,
      Category category,
      ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingDetails
          blockingDetails) {
    return BlockingPolicyData.builder()
        .status(status)
        .info(info)
        .timestamp(timestamp)
        .ruleType(ruleType)
        .category(category)
        .blockingDetails(blockingDetails)
        .ruleId("id")
        .build();
  }

  private static Builder setBlockingDetails(
      BlockingStatus status,
      String info,
      BlockingRuleType type,
      Optional<Long> timestamp,
      BlockingCategory category) {
    Builder builder =
        BlockingDetails.newBuilder()
            .setCategory(category)
            .setStatus(status)
            .setInfo(info)
            .setBlockingRuleType(type);
    return timestamp.map(builder::setExpirationTimestamp).orElse(builder);
  }
}
