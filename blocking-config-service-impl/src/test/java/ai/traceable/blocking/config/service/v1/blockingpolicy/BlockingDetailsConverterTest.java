package ai.traceable.blocking.config.service.v1.blockingpolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.BlockingPolicyDataBuilder;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Category;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyData.Status;
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
  private static final BlockingDetailsConverter converter = new BlockingDetailsConverter();

  @Test
  void testIpRangeConversion() {
    BlockingPolicyDataBuilder baseBuilder =
        setBlockingDetailsInfo(
            Status.ALLOWED, "ip-range", RuleType.ALLOW, 100L, Category.CUSTOM_IP_RULE);
    baseBuilder.ipAddresses(List.of("1.2.2.3", "1.2.3.4"));
    baseBuilder.ipRanges(List.of("11.22.33.44/5", "1.2.3.4/5"));

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

    assertEquals(List.of(detailsBuilder.build()), converter.convert(baseBuilder.build()));
  }

  @Test
  void testIpTypeConversion() {
    BlockingPolicyDataBuilder baseBuilder =
        setBlockingDetailsInfo(Status.DENIED, "ip-type", RuleType.BLOCK, 0L, Category.IP_TYPE_RULE);
    baseBuilder.ipTypes(
        List.of(
            IpLocationType.IP_LOCATION_TYPE_BOT, IpLocationType.IP_LOCATION_TYPE_TOR_EXIT_NODE));

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

    assertEquals(List.of(detailsBuilder.build()), converter.convert(baseBuilder.build()));
  }

  @Test
  void testRegionConversion() {
    BlockingPolicyDataBuilder baseBuilder =
        setBlockingDetailsInfo(
            Status.SUSPENDED,
            "region-type",
            RuleType.BLOCK_ALL_EXCEPT,
            100L,
            Category.CUSTOM_REGION_RULE);
    baseBuilder.regions(List.of("Spain", "Morocco"));

    Builder detailsBuilder =
        setBlockingDetails(
            BlockingStatus.BLOCKING_STATUS_SUSPENDED,
            "region-type",
            BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK_ALL_EXCEPT,
            Optional.of(100L),
            BlockingCategory.BLOCKING_CATEGORY_CUSTOM_REGION_RULE);
    detailsBuilder.setRegionDetails(
        RegionDetails.newBuilder().addAllRegions(List.of("Spain", "Morocco")).build());

    assertEquals(List.of(detailsBuilder.build()), converter.convert(baseBuilder.build()));
  }

  @Test
  void testModsecConversion() {
    BlockingPolicyDataBuilder baseBuilder =
        setBlockingDetailsInfo(
            Status.SUSPENDED, "modsec-type", RuleType.BLOCK, 0L, Category.MODSECURITY);
    baseBuilder.ruleId("crs_123");

    Builder detailsBuilder =
        setBlockingDetails(
            BlockingStatus.BLOCKING_STATUS_SUSPENDED,
            "modsec-type",
            BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK,
            Optional.empty(),
            BlockingCategory.BLOCKING_CATEGORY_MODSECURITY);
    detailsBuilder.setModsecDetails(ModsecDetails.newBuilder().setRuleId("crs_123").build());

    assertEquals(List.of(detailsBuilder.build()), converter.convert(baseBuilder.build()));
  }

  @Test
  void testCustomSignatureConversion() {
    BlockingPolicyDataBuilder baseBuilder =
        setBlockingDetailsInfo(
            Status.SNOOZED,
            "custom-signature-type",
            RuleType.ALLOW,
            100L,
            Category.CUSTOM_SIGNATURE_RULE);
    baseBuilder.ruleId("cs-1");

    Builder detailsBuilder =
        setBlockingDetails(
            BlockingStatus.BLOCKING_STATUS_SNOOZED,
            "custom-signature-type",
            BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW,
            Optional.of(100L),
            BlockingCategory.BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE);
    detailsBuilder.setCustomSignatureDetails(
        CustomSignatureDetails.newBuilder().setRuleId("cs-1").build());

    assertEquals(List.of(detailsBuilder.build()), converter.convert(baseBuilder.build()));
  }

  @Test
  void testThreatActorConversion() {
    BlockingPolicyDataBuilder baseBuilder =
        setBlockingDetailsInfo(
            Status.SUSPENDED, "threat-actor-type", RuleType.BLOCK, 100L, Category.THREAT_ACTOR);
    baseBuilder.userId("user-1");
    baseBuilder.ipAddresses(List.of("1.2.2.3", "1.2.3.4"));

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
    BlockingDetails actorDetails = detailsBuilder.build();
    detailsBuilder.clearActorDetails();
    detailsBuilder.setIpDetails(
        IpDetails.newBuilder().addAllIpAddresses(List.of("1.2.2.3", "1.2.3.4")).build());

    assertEquals(
        List.of(actorDetails, detailsBuilder.build()), converter.convert(baseBuilder.build()));
  }

  @Test
  void testRateLimitConversion() {
    BlockingPolicyDataBuilder baseBuilder =
        setBlockingDetailsInfo(
            Status.SUSPENDED, "rate-limit-type", RuleType.BLOCK, 100L, Category.RATE_LIMIT);
    baseBuilder.userId("user-1");
    baseBuilder.ipAddresses(List.of("1.2.2.3", "1.2.3.4"));

    Builder detailsBuilder =
        setBlockingDetails(
            BlockingStatus.BLOCKING_STATUS_SUSPENDED,
            "rate-limit-type",
            BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK,
            Optional.of(100L),
            BlockingCategory.BLOCKING_CATEGORY_RATE_LIMIT);
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
        List.of(actorDetails, detailsBuilder.build()), converter.convert(baseBuilder.build()));
  }

  @Test
  void testDataExfilConversion() {
    BlockingPolicyDataBuilder baseBuilder =
        setBlockingDetailsInfo(
            Status.DENIED, "data-exfil-type", RuleType.BLOCK, 100L, Category.DATA_EXFILTRATION);
    baseBuilder.userId("user-1");
    baseBuilder.ipAddresses(List.of("1.2.2.3", "1.2.3.4"));

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
        List.of(actorDetails, detailsBuilder.build()), converter.convert(baseBuilder.build()));
  }

  @Test
  void testEnumerationConversion() {
    BlockingPolicyDataBuilder baseBuilder =
        setBlockingDetailsInfo(
            Status.SUSPENDED, "enumeration-type", RuleType.BLOCK, 100L, Category.ENUMERATION);
    baseBuilder.userId("user-1");
    baseBuilder.ipAddresses(List.of("1.2.2.3", "1.2.3.4"));

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
        List.of(actorDetails, detailsBuilder.build()), converter.convert(baseBuilder.build()));
  }

  @Test
  void testEmailDomainConversion() {
    BlockingPolicyDataBuilder baseBuilder =
        setBlockingDetailsInfo(
            Status.SUSPENDED,
            "email-domain-type",
            RuleType.BLOCK,
            100L,
            Category.EMAIL_DOMAIN_RULE);
    baseBuilder.userId("user-1");
    baseBuilder.ipAddresses(List.of("1.2.2.3", "1.2.3.4"));

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
    BlockingDetails actorDetails = detailsBuilder.build();
    detailsBuilder.clearActorDetails();
    detailsBuilder.setIpDetails(
        IpDetails.newBuilder().addAllIpAddresses(List.of("1.2.2.3", "1.2.3.4")).build());

    assertEquals(
        List.of(actorDetails, detailsBuilder.build()), converter.convert(baseBuilder.build()));
  }

  private static BlockingPolicyDataBuilder setBlockingDetailsInfo(
      Status status, String info, RuleType ruleType, Long timestamp, Category category) {
    BlockingPolicyDataBuilder blockingPolicyDataBuilder = BlockingPolicyData.builder();
    blockingPolicyDataBuilder.status(status);
    blockingPolicyDataBuilder.info(info);
    blockingPolicyDataBuilder.timestamp(timestamp);
    blockingPolicyDataBuilder.ruleType(ruleType);
    blockingPolicyDataBuilder.category(category);
    return blockingPolicyDataBuilder;
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
