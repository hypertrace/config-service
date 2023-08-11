package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.malicioussources.config.service.v1.EventSeverity.EVENT_SEVERITY_CRITICAL;
import static ai.traceable.malicioussources.config.service.v1.IpLocationType.IP_LOCATION_TYPE_HOSTING_PROVIDER;
import static ai.traceable.malicioussources.config.service.v1.IpLocationType.IP_LOCATION_TYPE_PUBLIC_PROXY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpTypeBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.RegionBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.handlers.IpRangeDataHandler;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.handlers.IpTypeDataHandler;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.malicioussources.handlers.RegionDataHandler;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.malicioussources.config.service.v1.ExpirationDetails;
import ai.traceable.malicioussources.config.service.v1.GetMaliciousSourcesRulesResponse;
import ai.traceable.malicioussources.config.service.v1.IpAddressCondition;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.IpLocationTypeCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesConfigServiceGrpc;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleAction;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import ai.traceable.malicioussources.config.service.v1.Region;
import ai.traceable.malicioussources.config.service.v1.RegionCondition;
import ai.traceable.malicioussources.config.service.v1.RuleActionType;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MaliciousSourceRuleBlockingPolicyDataFetcherTest {

  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final long inactiveTimestamp = System.currentTimeMillis() - 10000L;
  private static final long activeTimestamp = System.currentTimeMillis() + 10000L;
  private MaliciousSourcesBlockingPolicyDataFetcher maliciousSourceRuleDataFetcher;
  private MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub
      maliciousSourcesConfigServiceBlockingStub;

  @BeforeEach
  void setUp() {
    this.maliciousSourcesConfigServiceBlockingStub =
        mock(MaliciousSourcesConfigServiceGrpc.MaliciousSourcesConfigServiceBlockingStub.class);
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

    maliciousSourceRuleDataFetcher =
        new MaliciousSourcesBlockingPolicyDataFetcher(
            maliciousSourcesConfigServiceBlockingStub,
            Map.of(
                MaliciousSourcesRuleCondition.ConditionCase.IP_RANGE_CONDITION,
                new IpRangeDataHandler(blockingRulesUtils),
                MaliciousSourcesRuleCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION,
                new IpTypeDataHandler(blockingRulesUtils),
                MaliciousSourcesRuleCondition.ConditionCase.REGION_CONDITION,
                new RegionDataHandler(blockingRulesUtils)));
  }

  @Test
  void ipTypeViolationsTest() {

    when((maliciousSourcesConfigServiceBlockingStub.getMaliciousSourcesRules(any())))
        .thenReturn(
            GetMaliciousSourcesRulesResponse.newBuilder()
                .addAllRules(
                    List.of(
                        generateIpTypeRule(
                            "1",
                            List.of(IP_LOCATION_TYPE_HOSTING_PROVIDER),
                            Timestamp.getDefaultInstance()),
                        generateIpTypeRule(
                            "2",
                            List.of(
                                IP_LOCATION_TYPE_HOSTING_PROVIDER, IP_LOCATION_TYPE_PUBLIC_PROXY),
                            Timestamp.newBuilder()
                                .setSeconds(activeTimestamp / 1000)
                                .setNanos(((int) (activeTimestamp % 1000)) * 1000000)
                                .build()),
                        generateIpTypeRule(
                            "3",
                            List.of(
                                IpLocationType.IP_LOCATION_TYPE_BOT, IP_LOCATION_TYPE_PUBLIC_PROXY),
                            Timestamp.newBuilder()
                                .setSeconds(inactiveTimestamp / 1000)
                                .setNanos(((int) (inactiveTimestamp % 1000)) * 1000000)
                                .build())))
                .build());

    List<BlockingPolicyData> maliciousSourceIpTypeViolations =
        maliciousSourceRuleDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder()
                    .environmentId(Optional.of(ENVIRONMENT_ID))
                    .build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList();
    assertEquals(2, maliciousSourceIpTypeViolations.size());
    assertEquals(
        IpTypeBlockingDetails.builder().ipTypes(List.of(IP_LOCATION_TYPE_HOSTING_PROVIDER)).build(),
        maliciousSourceIpTypeViolations.get(0).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.IP_TYPE_RULE,
        maliciousSourceIpTypeViolations.get(0).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.BLOCK, maliciousSourceIpTypeViolations.get(0).getRuleType());
    assertEquals(
        BlockingPolicyData.Status.DENIED, maliciousSourceIpTypeViolations.get(0).getStatus());
    assertEquals(0, maliciousSourceIpTypeViolations.get(0).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            "1",
            "rule-1",
            EVENT_SEVERITY_CRITICAL.name(),
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION)),
        maliciousSourceIpTypeViolations.get(0).getInfo());

    assertEquals(
        IpTypeBlockingDetails.builder()
            .ipTypes(List.of(IP_LOCATION_TYPE_HOSTING_PROVIDER, IP_LOCATION_TYPE_PUBLIC_PROXY))
            .build(),
        maliciousSourceIpTypeViolations.get(1).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.IP_TYPE_RULE,
        maliciousSourceIpTypeViolations.get(1).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.BLOCK, maliciousSourceIpTypeViolations.get(1).getRuleType());
    assertEquals(
        BlockingPolicyData.Status.SUSPENDED, maliciousSourceIpTypeViolations.get(1).getStatus());
    assertEquals(activeTimestamp, maliciousSourceIpTypeViolations.get(1).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            "2",
            "rule-2",
            EVENT_SEVERITY_CRITICAL.name(),
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_LOCATION_TYPE_CONDITION)),
        maliciousSourceIpTypeViolations.get(1).getInfo());
  }

  @Test
  void ipRangeViolationsTest() {
    when(maliciousSourcesConfigServiceBlockingStub.getMaliciousSourcesRules(any()))
        .thenReturn(
            GetMaliciousSourcesRulesResponse.newBuilder()
                .addAllRules(
                    List.of(
                        generateIpRangeRule(
                            "1",
                            Timestamp.getDefaultInstance(),
                            List.of("1.2.3.4/5", "1.2.3.4/5"),
                            List.of("1.2.3.4", "1.2.3.4"),
                            RuleActionType.RULE_ACTION_TYPE_BLOCK),
                        generateIpRangeRule(
                            "2",
                            Timestamp.newBuilder()
                                .setSeconds(activeTimestamp / 1000)
                                .setNanos(((int) (activeTimestamp % 1000)) * 1000000)
                                .build(),
                            List.of("1.2.3.4/6", "1.2.3.4/7"),
                            List.of("1.2.3.4", "1.2.3.5"),
                            RuleActionType.RULE_ACTION_TYPE_BLOCK),
                        generateIpRangeRule(
                            "3",
                            Timestamp.newBuilder()
                                .setSeconds(inactiveTimestamp / 1000)
                                .setNanos(((int) (inactiveTimestamp % 1000)) * 1000000)
                                .build(),
                            List.of("1.2.3.4/6"),
                            List.of("1.2.3.5"),
                            RuleActionType.RULE_ACTION_TYPE_BLOCK)))
                .build());

    List<BlockingPolicyData> maliciousSourceIpRangeViolations =
        maliciousSourceRuleDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder()
                    .environmentId(Optional.of(ENVIRONMENT_ID))
                    .build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList();

    assertEquals(2, maliciousSourceIpRangeViolations.size());
    assertEquals(
        IpBlockingDetails.builder()
            .ipRanges(List.of("1.2.3.4/5"))
            .ipAddresses(List.of("1.2.3.4"))
            .build(),
        maliciousSourceIpRangeViolations.get(0).getBlockingDetails());
    assertEquals(
        IpBlockingDetails.builder()
            .ipRanges(List.of("1.2.3.4/6", "1.2.3.4/7"))
            .ipAddresses(List.of("1.2.3.4", "1.2.3.5"))
            .build(),
        maliciousSourceIpRangeViolations.get(1).getBlockingDetails());

    assertEquals(
        BlockingPolicyData.Category.CUSTOM_IP_RULE,
        maliciousSourceIpRangeViolations.get(0).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.BLOCK, maliciousSourceIpRangeViolations.get(0).getRuleType());
    assertEquals(
        BlockingPolicyData.Status.DENIED, maliciousSourceIpRangeViolations.get(0).getStatus());
    assertEquals(0, maliciousSourceIpRangeViolations.get(0).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            "1",
            "rule-1",
            EVENT_SEVERITY_CRITICAL.name(),
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_RANGE_CONDITION)),
        maliciousSourceIpRangeViolations.get(0).getInfo());

    assertEquals(
        BlockingPolicyData.Category.CUSTOM_IP_RULE,
        maliciousSourceIpRangeViolations.get(1).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.BLOCK, maliciousSourceIpRangeViolations.get(1).getRuleType());
    assertEquals(
        BlockingPolicyData.Status.SUSPENDED, maliciousSourceIpRangeViolations.get(1).getStatus());
    assertEquals(activeTimestamp, maliciousSourceIpRangeViolations.get(1).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            "2",
            "rule-2",
            EVENT_SEVERITY_CRITICAL.name(),
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_RANGE_CONDITION)),
        maliciousSourceIpRangeViolations.get(1).getInfo());
  }

  @Test
  void ipRangeExemptionsTest() {
    when(maliciousSourcesConfigServiceBlockingStub.getMaliciousSourcesRules(any()))
        .thenReturn(
            GetMaliciousSourcesRulesResponse.newBuilder()
                .addAllRules(
                    List.of(
                        generateIpRangeRule(
                            "1",
                            Timestamp.getDefaultInstance(),
                            List.of("1.2.3.4/5", "1.2.3.4/5"),
                            List.of("1.2.3.4", "1.2.3.4"),
                            RuleActionType.RULE_ACTION_TYPE_ALLOW),
                        generateIpRangeRule(
                            "2",
                            Timestamp.newBuilder()
                                .setSeconds(activeTimestamp / 1000)
                                .setNanos(((int) (activeTimestamp % 1000)) * 1000000)
                                .build(),
                            List.of("1.2.3.4/6", "1.2.3.4/7"),
                            List.of("1.2.3.4", "1.2.3.5"),
                            RuleActionType.RULE_ACTION_TYPE_ALLOW),
                        generateIpRangeRule(
                            "3",
                            Timestamp.newBuilder()
                                .setSeconds(inactiveTimestamp / 1000)
                                .setNanos(((int) (inactiveTimestamp % 1000)) * 1000000)
                                .build(),
                            List.of("1.2.3.4/6"),
                            List.of("1.2.3.5"),
                            RuleActionType.RULE_ACTION_TYPE_ALLOW)))
                .build());
    List<BlockingPolicyData> maliciousSourceIpRangeExemptions =
        maliciousSourceRuleDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder()
                    .environmentId(Optional.of(ENVIRONMENT_ID))
                    .build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList();
    assertEquals(2, maliciousSourceIpRangeExemptions.size());
    assertEquals(
        IpBlockingDetails.builder()
            .ipRanges(List.of("1.2.3.4/5"))
            .ipAddresses(List.of("1.2.3.4"))
            .build(),
        maliciousSourceIpRangeExemptions.get(0).getBlockingDetails());
    assertEquals(
        IpBlockingDetails.builder()
            .ipRanges(List.of("1.2.3.4/6", "1.2.3.4/7"))
            .ipAddresses(List.of("1.2.3.4", "1.2.3.5"))
            .build(),
        maliciousSourceIpRangeExemptions.get(1).getBlockingDetails());

    assertEquals(
        BlockingPolicyData.Category.CUSTOM_IP_RULE,
        maliciousSourceIpRangeExemptions.get(0).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.ALLOW, maliciousSourceIpRangeExemptions.get(0).getRuleType());
    assertEquals(
        BlockingPolicyData.Status.ALLOWED, maliciousSourceIpRangeExemptions.get(0).getStatus());
    assertEquals(0, maliciousSourceIpRangeExemptions.get(0).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            "1",
            "rule-1",
            EVENT_SEVERITY_CRITICAL.name(),
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_RANGE_CONDITION)),
        maliciousSourceIpRangeExemptions.get(0).getInfo());

    assertEquals(
        BlockingPolicyData.Category.CUSTOM_IP_RULE,
        maliciousSourceIpRangeExemptions.get(1).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.ALLOW, maliciousSourceIpRangeExemptions.get(1).getRuleType());
    assertEquals(
        BlockingPolicyData.Status.SNOOZED, maliciousSourceIpRangeExemptions.get(1).getStatus());
    assertEquals(activeTimestamp, maliciousSourceIpRangeExemptions.get(1).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            "2",
            "rule-2",
            EVENT_SEVERITY_CRITICAL.name(),
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_RANGE_CONDITION)),
        maliciousSourceIpRangeExemptions.get(1).getInfo());
  }

  @Test
  void ipRangeBlockAllExceptTest() {

    when(maliciousSourcesConfigServiceBlockingStub.getMaliciousSourcesRules(any()))
        .thenReturn(
            GetMaliciousSourcesRulesResponse.newBuilder()
                .addAllRules(
                    List.of(
                        generateIpRangeRule(
                            "1",
                            Timestamp.getDefaultInstance(),
                            List.of("1.2.3.4/5", "1.2.3.4/5"),
                            List.of("1.2.3.4", "1.2.3.4"),
                            RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT),
                        generateIpRangeRule(
                            "2",
                            Timestamp.newBuilder()
                                .setSeconds(activeTimestamp / 1000)
                                .setNanos(((int) (activeTimestamp % 1000)) * 1000000)
                                .build(),
                            List.of("1.2.3.4/6", "1.2.3.4/7"),
                            List.of("1.2.3.4", "1.2.3.5"),
                            RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT),
                        generateIpRangeRule(
                            "3",
                            Timestamp.newBuilder()
                                .setSeconds(inactiveTimestamp / 1000)
                                .setNanos(((int) (inactiveTimestamp % 1000)) * 1000000)
                                .build(),
                            List.of("1.2.3.4/6"),
                            List.of("1.2.3.5"),
                            RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)))
                .build());

    List<BlockingPolicyData> maliciousSourceIpRangeBlockAllExcept =
        maliciousSourceRuleDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder()
                    .environmentId(Optional.of(ENVIRONMENT_ID))
                    .build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList();

    assertEquals(2, maliciousSourceIpRangeBlockAllExcept.size());
    assertEquals(
        IpBlockingDetails.builder()
            .ipRanges(List.of("1.2.3.4/5"))
            .ipAddresses(List.of("1.2.3.4"))
            .build(),
        maliciousSourceIpRangeBlockAllExcept.get(0).getBlockingDetails());
    assertEquals(
        IpBlockingDetails.builder()
            .ipRanges(List.of("1.2.3.4/6", "1.2.3.4/7"))
            .ipAddresses(List.of("1.2.3.4", "1.2.3.5"))
            .build(),
        maliciousSourceIpRangeBlockAllExcept.get(1).getBlockingDetails());

    assertEquals(
        BlockingPolicyData.Category.CUSTOM_IP_RULE,
        maliciousSourceIpRangeBlockAllExcept.get(0).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.BLOCK_ALL_EXCEPT,
        maliciousSourceIpRangeBlockAllExcept.get(0).getRuleType());
    assertEquals(
        BlockingPolicyData.Status.DENIED, maliciousSourceIpRangeBlockAllExcept.get(0).getStatus());
    assertEquals(0, maliciousSourceIpRangeBlockAllExcept.get(0).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            "1",
            "rule-1",
            EVENT_SEVERITY_CRITICAL.name(),
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_RANGE_CONDITION)),
        maliciousSourceIpRangeBlockAllExcept.get(0).getInfo());

    assertEquals(
        BlockingPolicyData.Category.CUSTOM_IP_RULE,
        maliciousSourceIpRangeBlockAllExcept.get(1).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.BLOCK_ALL_EXCEPT,
        maliciousSourceIpRangeBlockAllExcept.get(1).getRuleType());
    assertEquals(
        BlockingPolicyData.Status.SUSPENDED,
        maliciousSourceIpRangeBlockAllExcept.get(1).getStatus());
    assertEquals(activeTimestamp, maliciousSourceIpRangeBlockAllExcept.get(1).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            "2",
            "rule-2",
            EVENT_SEVERITY_CRITICAL.name(),
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.IP_RANGE_CONDITION)),
        maliciousSourceIpRangeBlockAllExcept.get(1).getInfo());
  }

  @Test
  void regionViolationsTest() {
    when(maliciousSourcesConfigServiceBlockingStub.getMaliciousSourcesRules(any()))
        .thenReturn(
            GetMaliciousSourcesRulesResponse.newBuilder()
                .addAllRules(
                    List.of(
                        generateRegionRule(
                            "1",
                            Timestamp.getDefaultInstance(),
                            List.of(
                                Region.newBuilder().setCountryIsoCode("AFG").build(),
                                Region.newBuilder().setCountryIsoCode("IND").build(),
                                Region.newBuilder().setCountryIsoCode("IND").build()),
                            RuleActionType.RULE_ACTION_TYPE_BLOCK),
                        generateRegionRule(
                            "2",
                            Timestamp.newBuilder()
                                .setSeconds(activeTimestamp / 1000)
                                .setNanos(((int) (activeTimestamp % 1000)) * 1000000)
                                .build(),
                            List.of(Region.newBuilder().setCountryIsoCode("AFG").build()),
                            RuleActionType.RULE_ACTION_TYPE_BLOCK),
                        generateRegionRule(
                            "3",
                            Timestamp.newBuilder()
                                .setSeconds(inactiveTimestamp / 1000)
                                .setNanos(((int) (inactiveTimestamp % 1000)) * 1000000)
                                .build(),
                            List.of(Region.newBuilder().setCountryIsoCode("AFG").build()),
                            RuleActionType.RULE_ACTION_TYPE_BLOCK)))
                .build());

    List<BlockingPolicyData> maliciousSourceRegionViolations =
        maliciousSourceRuleDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder()
                    .environmentId(Optional.of(ENVIRONMENT_ID))
                    .build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList();
    assertEquals(2, maliciousSourceRegionViolations.size());
    assertEquals(
        RegionBlockingDetails.builder().regions(List.of("AFG", "IND")).build(),
        maliciousSourceRegionViolations.get(0).getBlockingDetails());
    assertEquals(
        RegionBlockingDetails.builder().regions(List.of("AFG")).build(),
        maliciousSourceRegionViolations.get(1).getBlockingDetails());

    assertEquals(
        BlockingPolicyData.Category.CUSTOM_REGION_RULE,
        maliciousSourceRegionViolations.get(0).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.BLOCK, maliciousSourceRegionViolations.get(0).getRuleType());
    assertEquals(
        BlockingPolicyData.Status.DENIED, maliciousSourceRegionViolations.get(0).getStatus());
    assertEquals(0, maliciousSourceRegionViolations.get(0).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            "1",
            "rule-1",
            EVENT_SEVERITY_CRITICAL.name(),
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.REGION_CONDITION)),
        maliciousSourceRegionViolations.get(0).getInfo());

    assertEquals(
        BlockingPolicyData.Category.CUSTOM_REGION_RULE,
        maliciousSourceRegionViolations.get(1).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.BLOCK, maliciousSourceRegionViolations.get(1).getRuleType());
    assertEquals(
        BlockingPolicyData.Status.SUSPENDED, maliciousSourceRegionViolations.get(1).getStatus());
    assertEquals(activeTimestamp, maliciousSourceRegionViolations.get(1).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            "2",
            "rule-2",
            EVENT_SEVERITY_CRITICAL.name(),
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.REGION_CONDITION)),
        maliciousSourceRegionViolations.get(1).getInfo());
  }

  @Test
  void regionBlockAllExcept() {
    when(maliciousSourcesConfigServiceBlockingStub.getMaliciousSourcesRules(any()))
        .thenReturn(
            GetMaliciousSourcesRulesResponse.newBuilder()
                .addAllRules(
                    List.of(
                        generateRegionRule(
                            "1",
                            Timestamp.getDefaultInstance(),
                            List.of(
                                Region.newBuilder().setCountryIsoCode("AFG").build(),
                                Region.newBuilder().setCountryIsoCode("IND").build(),
                                Region.newBuilder().setCountryIsoCode("IND").build()),
                            RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT),
                        generateRegionRule(
                            "2",
                            Timestamp.newBuilder()
                                .setSeconds(activeTimestamp / 1000)
                                .setNanos(((int) (activeTimestamp % 1000)) * 1000000)
                                .build(),
                            List.of(Region.newBuilder().setCountryIsoCode("AFG").build()),
                            RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT),
                        generateRegionRule(
                            "3",
                            Timestamp.newBuilder()
                                .setSeconds(inactiveTimestamp / 1000)
                                .setNanos(((int) (inactiveTimestamp % 1000)) * 1000000)
                                .build(),
                            List.of(Region.newBuilder().setCountryIsoCode("AFG").build()),
                            RuleActionType.RULE_ACTION_TYPE_BLOCK_ALL_EXCEPT)))
                .build());

    List<BlockingPolicyData> maliciousSourceRegionBlockAllExcept =
        maliciousSourceRuleDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder()
                    .environmentId(Optional.of(ENVIRONMENT_ID))
                    .build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList();

    assertEquals(2, maliciousSourceRegionBlockAllExcept.size());
    assertEquals(
        RegionBlockingDetails.builder().regions(List.of("AFG", "IND")).build(),
        maliciousSourceRegionBlockAllExcept.get(0).getBlockingDetails());
    assertEquals(
        RegionBlockingDetails.builder().regions(List.of("AFG")).build(),
        maliciousSourceRegionBlockAllExcept.get(1).getBlockingDetails());

    assertEquals(
        BlockingPolicyData.Category.CUSTOM_REGION_RULE,
        maliciousSourceRegionBlockAllExcept.get(0).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.BLOCK_ALL_EXCEPT,
        maliciousSourceRegionBlockAllExcept.get(0).getRuleType());
    assertEquals(
        BlockingPolicyData.Status.DENIED, maliciousSourceRegionBlockAllExcept.get(0).getStatus());
    assertEquals(0, maliciousSourceRegionBlockAllExcept.get(0).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            "1",
            "rule-1",
            EVENT_SEVERITY_CRITICAL.name(),
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.REGION_CONDITION)),
        maliciousSourceRegionBlockAllExcept.get(0).getInfo());

    assertEquals(
        BlockingPolicyData.Category.CUSTOM_REGION_RULE,
        maliciousSourceRegionBlockAllExcept.get(1).getCategory());
    assertEquals(
        BlockingPolicyData.RuleType.BLOCK_ALL_EXCEPT,
        maliciousSourceRegionBlockAllExcept.get(1).getRuleType());
    assertEquals(
        BlockingPolicyData.Status.SUSPENDED,
        maliciousSourceRegionBlockAllExcept.get(1).getStatus());
    assertEquals(activeTimestamp, maliciousSourceRegionBlockAllExcept.get(1).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedMaliciousSourcesViolationInfo(
            "2",
            "rule-2",
            EVENT_SEVERITY_CRITICAL.name(),
            Optional.empty(),
            List.of(MaliciousSourcesRuleCondition.ConditionCase.REGION_CONDITION)),
        maliciousSourceRegionBlockAllExcept.get(1).getInfo());
  }

  private static MaliciousSourcesRule generateIpTypeRule(
      String id, List<IpLocationType> ipLocationTypeList, Timestamp timestamp) {
    MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
        MaliciousSourcesRuleInfo.newBuilder()
            .setName("rule-" + id)
            .setDescription("test rule-" + id)
            .addConditions(
                MaliciousSourcesRuleCondition.newBuilder()
                    .setIpLocationTypeCondition(
                        IpLocationTypeCondition.newBuilder()
                            .addAllIpLocationTypes(ipLocationTypeList)
                            .build()))
            .setRuleAction(
                MaliciousSourcesRuleAction.newBuilder()
                    .setActionType(RuleActionType.RULE_ACTION_TYPE_BLOCK)
                    .setEventSeverity(EVENT_SEVERITY_CRITICAL)
                    .setExpirationDetails(
                        ExpirationDetails.newBuilder().setExpirationTimestamp(timestamp))
                    .build())
            .build();

    return MaliciousSourcesRule.newBuilder()
        .setId(id)
        .setRuleInfo(maliciousSourcesRuleInfo)
        .build();
  }

  private static MaliciousSourcesRule generateIpRangeRule(
      String id,
      Timestamp timestamp,
      List<String> cidrIpRange,
      List<String> ipAddress,
      RuleActionType ruleActionType) {
    MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
        MaliciousSourcesRuleInfo.newBuilder()
            .setName("rule-" + id)
            .setDescription("test rule-" + id)
            .addConditions(
                MaliciousSourcesRuleCondition.newBuilder()
                    .setIpRangeCondition(
                        IpAddressCondition.newBuilder()
                            .addAllCidrIpRanges(cidrIpRange)
                            .addAllIpAddresses(ipAddress)
                            .build())
                    .build())
            .setRuleAction(
                MaliciousSourcesRuleAction.newBuilder()
                    .setActionType(ruleActionType)
                    .setEventSeverity(EVENT_SEVERITY_CRITICAL)
                    .setExpirationDetails(
                        ExpirationDetails.newBuilder().setExpirationTimestamp(timestamp))
                    .build())
            .build();

    return MaliciousSourcesRule.newBuilder()
        .setId(id)
        .setRuleInfo(maliciousSourcesRuleInfo)
        .build();
  }

  private static MaliciousSourcesRule generateRegionRule(
      String id, Timestamp timestamp, List<Region> region, RuleActionType ruleActionType) {
    MaliciousSourcesRuleInfo maliciousSourcesRuleInfo =
        MaliciousSourcesRuleInfo.newBuilder()
            .setName("rule-" + id)
            .setDescription("test rule-" + id)
            .addConditions(
                MaliciousSourcesRuleCondition.newBuilder()
                    .setRegionCondition(RegionCondition.newBuilder().addAllRegions(region).build())
                    .build())
            .setRuleAction(
                MaliciousSourcesRuleAction.newBuilder()
                    .setActionType(ruleActionType)
                    .setEventSeverity(EVENT_SEVERITY_CRITICAL)
                    .setExpirationDetails(
                        ExpirationDetails.newBuilder().setExpirationTimestamp(timestamp))
                    .build())
            .build();

    return MaliciousSourcesRule.newBuilder()
        .setId(id)
        .setRuleInfo(maliciousSourcesRuleInfo)
        .build();
  }
}
