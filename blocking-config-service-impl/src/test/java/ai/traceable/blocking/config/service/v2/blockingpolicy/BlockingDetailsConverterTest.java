package ai.traceable.blocking.config.service.v2.blockingpolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import ai.traceable.blocking.config.service.v2.ActorDetails;
import ai.traceable.blocking.config.service.v2.BlockingCategory;
import ai.traceable.blocking.config.service.v2.BlockingDetails;
import ai.traceable.blocking.config.service.v2.BlockingDetails.Builder;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCombination;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCombination.ConditionsOperator;
import ai.traceable.blocking.config.service.v2.BlockingDetailsCondition;
import ai.traceable.blocking.config.service.v2.BlockingRuleType;
import ai.traceable.blocking.config.service.v2.BlockingStatus;
import ai.traceable.blocking.config.service.v2.CustomSignatureDetails;
import ai.traceable.blocking.config.service.v2.IpDetails;
import ai.traceable.blocking.config.service.v2.IpType;
import ai.traceable.blocking.config.service.v2.IpTypeDetails;
import ai.traceable.blocking.config.service.v2.ModsecDetails;
import ai.traceable.blocking.config.service.v2.RegionDetails;
import ai.traceable.blocking.config.service.v2.RuleAction;
import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class BlockingDetailsConverterTest {
  private static final BlockingDetailsConverter converter =
      new BlockingDetailsConverter(
          new ActorDetailsForThreatActorVisitor(),
          new IpDetailsForThreatActorVisitor(),
          new SemanticVersioningComparator());

  private static final BlockingPolicyDataFilter filter =
      BlockingPolicyDataFilter.builder().minLibtraceableVersion("0.1.98-rc.164").build();
  private static final RuleAction mockRuleAction = mock(RuleAction.class);

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
    BlockingDetails actorDetails = detailsBuilder.build();

    assertEquals(
        actorDetails,
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

    // Testing backward compatibility for older TAs
    assertEquals(
        detailsBuilder
            .clearActorDetails()
            .setIpDetails(
                IpDetails.newBuilder().addAllIpAddresses(List.of("1.2.2.3", "1.2.3.4")).build())
            .build(),
        converter.convert(
            setBlockingDetailsInfo(
                Status.SUSPENDED,
                "threat-actor-type",
                RuleType.BLOCK,
                100L,
                Category.THREAT_ACTOR,
                IpBlockingDetails.builder().ipAddresses(List.of("1.2.2.3", "1.2.3.4")).build()),
            BlockingPolicyDataFilter.builder().build()));
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
        actorDetails,
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
        actorDetails,
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
        actorDetails,
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
    BlockingDetails actorDetails = detailsBuilder.build();
    detailsBuilder.clearActorDetails();
    detailsBuilder.setIpDetails(
        IpDetails.newBuilder().addAllIpAddresses(List.of("1.2.2.3", "1.2.3.4")).build());

    assertEquals(
        actorDetails,
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
  void testComplexConditions() {
    BlockingPolicyData policy =
        setBlockingDetailsInfo(
            Status.SUSPENDED,
            "transaction-based-type",
            RuleType.BLOCK,
            100L,
            Category.DATA_EXFILTRATION,
            CombinationBlockingDetails.builder()
                .operator(Operator.AND)
                .blockingDetailsOperands(
                    List.of(
                        IpBlockingDetails.builder().ipAddresses(List.of("1.2.3.4")).build(),
                        RegionBlockingDetails.builder().regions(List.of("Bhutan")).build(),
                        IpTypeBlockingDetails.builder()
                            .ipTypes(List.of(IpLocationType.IP_LOCATION_TYPE_ANONYMOUS_VPN))
                            .build(),
                        CombinationBlockingDetails.builder()
                            .operator(Operator.OR)
                            .blockingDetailsOperands(
                                List.of(
                                    CustomSignatureBlockingDetails.builder()
                                        .ruleId("data-type-id-1")
                                        .build(),
                                    CombinationBlockingDetails.builder()
                                        .operator(Operator.AND)
                                        .blockingDetailsOperands(
                                            List.of(
                                                CustomSignatureBlockingDetails.builder()
                                                    .ruleId("data-type-id-2")
                                                    .build(),
                                                RegionBlockingDetails.builder()
                                                    .region("Nepal")
                                                    .build()))
                                        .build(),
                                    CustomSignatureBlockingDetails.builder()
                                        .ruleId("data-type-id-3")
                                        .build()))
                            .build(),
                        CustomSignatureBlockingDetails.builder()
                            .ruleId("url-regex-rule-id")
                            .build()))
                .build());

    Builder detailsBuilder =
        setBlockingDetails(
            BlockingStatus.BLOCKING_STATUS_SUSPENDED,
            "transaction-based-type",
            BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK,
            Optional.of(100L),
            BlockingCategory.BLOCKING_CATEGORY_DATA_EXFILTRATION);
    detailsBuilder.setDetailsCombination(
        BlockingDetailsCombination.newBuilder()
            .setOperator(ConditionsOperator.CONDITIONS_OPERATOR_AND)
            .addDetailsConditions(
                BlockingDetailsCondition.newBuilder()
                    .setIpDetails(IpDetails.newBuilder().addIpAddresses("1.2.3.4")))
            .addDetailsConditions(
                BlockingDetailsCondition.newBuilder()
                    .setRegionDetails(RegionDetails.newBuilder().addRegions("Bhutan")))
            .addDetailsConditions(
                BlockingDetailsCondition.newBuilder()
                    .setIpTypeDetails(IpTypeDetails.newBuilder().addIpTypes(IpType.IP_TYPE_VPN)))
            .addDetailsConditions(
                BlockingDetailsCondition.newBuilder()
                    .setDetailsCombination(
                        BlockingDetailsCombination.newBuilder()
                            .setOperator(ConditionsOperator.CONDITIONS_OPERATOR_OR)
                            .addDetailsConditions(
                                BlockingDetailsCondition.newBuilder()
                                    .setCustomSignatureDetails(
                                        CustomSignatureDetails.newBuilder()
                                            .setRuleId("data-type-id-1")))
                            .addDetailsConditions(
                                BlockingDetailsCondition.newBuilder()
                                    .setDetailsCombination(
                                        BlockingDetailsCombination.newBuilder()
                                            .setOperator(ConditionsOperator.CONDITIONS_OPERATOR_AND)
                                            .addDetailsConditions(
                                                BlockingDetailsCondition.newBuilder()
                                                    .setCustomSignatureDetails(
                                                        CustomSignatureDetails.newBuilder()
                                                            .setRuleId("data-type-id-2")))
                                            .addDetailsConditions(
                                                BlockingDetailsCondition.newBuilder()
                                                    .setRegionDetails(
                                                        RegionDetails.newBuilder()
                                                            .addRegions("Nepal")))))
                            .addDetailsConditions(
                                BlockingDetailsCondition.newBuilder()
                                    .setCustomSignatureDetails(
                                        CustomSignatureDetails.newBuilder()
                                            .setRuleId("data-type-id-3")))))
            .addDetailsConditions(
                BlockingDetailsCondition.newBuilder()
                    .setCustomSignatureDetails(
                        CustomSignatureDetails.newBuilder().setRuleId("url-regex-rule-id")))
            .build());
    assertEquals(detailsBuilder.build(), converter.convert(policy, filter));
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
        .action(mockRuleAction)
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
            .setBlockingRuleType(type)
            .setAction(mockRuleAction);
    return timestamp.map(builder::setExpirationTimestamp).orElse(builder);
  }
}
