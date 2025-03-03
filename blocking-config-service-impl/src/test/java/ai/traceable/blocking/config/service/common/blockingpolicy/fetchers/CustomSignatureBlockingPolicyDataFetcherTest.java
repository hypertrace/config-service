package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.customsignature.config.service.v1.EventSeverity.EVENT_SEVERITY_HIGH;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_ALLOW;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_DETECTION_AND_BLOCKING;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_TESTING_DETECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CombinationBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CombinationBlockingDetails.Operator;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CustomSignatureBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.IpTypeBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.RegionBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.customsignature.config.service.v1.AgentModification;
import ai.traceable.customsignature.config.service.v1.AgentRuleEffect;
import ai.traceable.customsignature.config.service.v1.Clause;
import ai.traceable.customsignature.config.service.v1.ClauseGroup;
import ai.traceable.customsignature.config.service.v1.ClauseOperator;
import ai.traceable.customsignature.config.service.v1.CustomSecRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureInlineRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.FieldValue;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.HeaderInjection;
import ai.traceable.customsignature.config.service.v1.IpAddressExpression;
import ai.traceable.customsignature.config.service.v1.IpType;
import ai.traceable.customsignature.config.service.v1.IpTypeExpression;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.MatchExpression;
import ai.traceable.customsignature.config.service.v1.MatchKey;
import ai.traceable.customsignature.config.service.v1.MatchOperator;
import ai.traceable.customsignature.config.service.v1.RegionExpression;
import ai.traceable.customsignature.config.service.v1.RegionExpression.Region;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleEffectWithModifications;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CustomSignatureBlockingPolicyDataFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "environment-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final GetCustomSignatureRulesRequest DEFAULT_GET_REQUEST =
      GetCustomSignatureRulesRequest.newBuilder()
          .setFilter(
              GetRulesFilter.newBuilder()
                  .setDisabled(false)
                  .setRuleScope(
                      RuleScope.newBuilder().setEnvironmentScope(EnvironmentScope.newBuilder())))
          .build();
  private static final Clause ipAddressClause =
      Clause.newBuilder()
          .setIpAddressExpression(
              IpAddressExpression.newBuilder().addIpAddresses("1.2.3.4").build())
          .build();
  private static final Clause customSecRuleClause =
      Clause.newBuilder()
          .setCustomSecRule(
              CustomSecRule.newBuilder()
                  .setInputSecRule(
                      "SecRule REQUEST_URI \"@contains admin\" \"id:1001,deny,status:403,msg:'Admin Access Attempt Blocked'\"\n")
                  .build())
          .build();

  private static final Clause matchClause =
      Clause.newBuilder()
          .setMatchExpression(
              MatchExpression.newBuilder()
                  .setMatchKey(MatchKey.MATCH_KEY_URL)
                  .setMatchOperator(MatchOperator.MATCH_OPERATOR_CONTAINS)
                  .setMatchValue("admin")
                  .setMatchCategory(MatchCategory.MATCH_CATEGORY_REQUEST)
                  .build())
          .build();
  private static final Clause ipTypeClause =
      Clause.newBuilder()
          .setIpTypeExpression(IpTypeExpression.newBuilder().addIpTypes(IpType.IP_TYPE_BOT))
          .build();
  private static final Clause regionClause =
      Clause.newBuilder()
          .setRegionExpression(
              RegionExpression.newBuilder()
                  .addRegionIdentifiers(Region.newBuilder().setCountryIsoCode("isoCode1").build()))
          .build();
  private CustomSignatureBlockingPolicyDataFetcher customSignatureDataFetcher;
  private BlockingRulesSupplier blockingRulesSupplier;
  private static final long inactiveTimestamp = System.currentTimeMillis() - 10000L;
  private static final long activeTimestamp = System.currentTimeMillis() + 10000L;

  @BeforeEach
  void setUp() {
    blockingRulesSupplier = mock(BlockingRulesSupplier.class);
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

    customSignatureDataFetcher = new CustomSignatureBlockingPolicyDataFetcher(blockingRulesUtils);
  }

  @Test
  void getCustomSignatureRulesTestEmpty() {
    doReturn(Collections.emptyList()).when(blockingRulesSupplier).getCustomSignatureInlineRules();
    List<BlockingPolicyData> customSignatureRuleList =
        customSignatureDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build(),
                blockingRulesSupplier)
            .getBlockingPolicyList();
    assertEquals(0, customSignatureRuleList.size());
  }

  @Test
  void getCustomSignatureRulesTest() {
    doReturn(sampleCustomSignatureAllEnvRules)
        .when(blockingRulesSupplier)
        .getCustomSignatureInlineRules();

    List<BlockingPolicyData> customSignatureRuleList =
        customSignatureDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build(),
                blockingRulesSupplier)
            .getBlockingPolicyList();

    assertEquals(3, customSignatureRuleList.size());

    assertEquals(
        CustomSignatureBlockingDetails.builder().ruleId("rule-id-1").build(),
        customSignatureRuleList.get(0).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_SIGNATURE_RULE,
        customSignatureRuleList.get(0).getCategory());
    assertEquals(BlockingPolicyData.RuleType.ALLOW, customSignatureRuleList.get(0).getRuleType());
    assertEquals(BlockingPolicyData.Status.SNOOZED, customSignatureRuleList.get(0).getStatus());
    assertEquals(activeTimestamp, customSignatureRuleList.get(0).getTimestamp());
    assertEquals(
        ExemptionInfoEncoder.getEncodedCustomSignatureRuleExemptionInfo(
            "rule-id-1", "rule-name-1", EVENT_SEVERITY_HIGH.name()),
        customSignatureRuleList.get(0).getInfo());
    assertEquals("rule-id-1", customSignatureRuleList.get(0).getRuleId());

    assertEquals(
        IpBlockingDetails.builder().ipAddress("1.2.3.4").build(),
        customSignatureRuleList.get(1).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_SIGNATURE_RULE,
        customSignatureRuleList.get(1).getCategory());
    assertEquals(BlockingPolicyData.RuleType.BLOCK, customSignatureRuleList.get(1).getRuleType());
    assertEquals(BlockingPolicyData.Status.SUSPENDED, customSignatureRuleList.get(1).getStatus());
    assertNull(customSignatureRuleList.get(1).getAction());
    assertEquals(activeTimestamp, customSignatureRuleList.get(1).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomSignatureRuleViolationInfo(
            "rule-id-3", "rule-name-3", EVENT_SEVERITY_HIGH.name(), Map.of("key", "value")),
        customSignatureRuleList.get(1).getInfo());
    assertEquals("rule-id-3", customSignatureRuleList.get(1).getRuleId());

    assertEquals(
        IpTypeBlockingDetails.builder().ipType(IpLocationType.IP_LOCATION_TYPE_BOT).build(),
        customSignatureRuleList.get(2).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_SIGNATURE_RULE,
        customSignatureRuleList.get(2).getCategory());
    assertEquals(BlockingPolicyData.RuleType.BLOCK, customSignatureRuleList.get(2).getRuleType());
    assertEquals(BlockingPolicyData.Status.SUSPENDED, customSignatureRuleList.get(2).getStatus());
    assertNull(customSignatureRuleList.get(2).getAction());
    assertEquals(activeTimestamp, customSignatureRuleList.get(2).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomSignatureRuleViolationInfo(
            "rule-id-7", "rule-name-7", EVENT_SEVERITY_HIGH.name(), Map.of("key", "value")),
        customSignatureRuleList.get(2).getInfo());
    assertEquals("rule-id-7", customSignatureRuleList.get(2).getRuleId());
  }

  @Test
  void getCustomSignatureRulesTestWithEnvironment() {
    List<CustomSignatureInlineRule> inlineRuleListForEnv =
        new ArrayList<>(sampleCustomSignatureAllEnvRules);
    inlineRuleListForEnv.addAll(sampleCustomSignatureEnvRules);

    doReturn(inlineRuleListForEnv).when(blockingRulesSupplier).getCustomSignatureInlineRules();

    List<BlockingPolicyData> customSignatureRuleList =
        customSignatureDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder()
                    .environmentId(Optional.of(ENVIRONMENT_ID))
                    .build(),
                blockingRulesSupplier)
            .getBlockingPolicyList();

    assertEquals(5, customSignatureRuleList.size());

    assertEquals(
        CustomSignatureBlockingDetails.builder().ruleId("rule-id-1").build(),
        customSignatureRuleList.get(0).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_SIGNATURE_RULE,
        customSignatureRuleList.get(0).getCategory());
    assertEquals(BlockingPolicyData.RuleType.ALLOW, customSignatureRuleList.get(0).getRuleType());
    assertEquals(BlockingPolicyData.Status.SNOOZED, customSignatureRuleList.get(0).getStatus());
    assertEquals(activeTimestamp, customSignatureRuleList.get(0).getTimestamp());
    assertEquals(
        ExemptionInfoEncoder.getEncodedCustomSignatureRuleExemptionInfo(
            "rule-id-1", "rule-name-1", EVENT_SEVERITY_HIGH.name()),
        customSignatureRuleList.get(0).getInfo());
    assertEquals("rule-id-1", customSignatureRuleList.get(0).getRuleId());

    assertEquals(
        IpBlockingDetails.builder().ipAddress("1.2.3.4").build(),
        customSignatureRuleList.get(1).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_SIGNATURE_RULE,
        customSignatureRuleList.get(1).getCategory());
    assertEquals(BlockingPolicyData.RuleType.BLOCK, customSignatureRuleList.get(1).getRuleType());
    assertEquals(BlockingPolicyData.Status.SUSPENDED, customSignatureRuleList.get(1).getStatus());
    assertNull(customSignatureRuleList.get(1).getAction());
    assertEquals(activeTimestamp, customSignatureRuleList.get(1).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomSignatureRuleViolationInfo(
            "rule-id-3", "rule-name-3", EVENT_SEVERITY_HIGH.name(), Map.of("key", "value")),
        customSignatureRuleList.get(1).getInfo());
    assertEquals("rule-id-3", customSignatureRuleList.get(1).getRuleId());

    assertEquals(
        IpTypeBlockingDetails.builder().ipType(IpLocationType.IP_LOCATION_TYPE_BOT).build(),
        customSignatureRuleList.get(2).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_SIGNATURE_RULE,
        customSignatureRuleList.get(2).getCategory());
    assertEquals(BlockingPolicyData.RuleType.BLOCK, customSignatureRuleList.get(2).getRuleType());
    assertEquals(BlockingPolicyData.Status.SUSPENDED, customSignatureRuleList.get(2).getStatus());
    assertNull(customSignatureRuleList.get(2).getAction());
    assertEquals(activeTimestamp, customSignatureRuleList.get(2).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomSignatureRuleViolationInfo(
            "rule-id-7", "rule-name-7", EVENT_SEVERITY_HIGH.name(), Map.of("key", "value")),
        customSignatureRuleList.get(2).getInfo());
    assertEquals("rule-id-7", customSignatureRuleList.get(2).getRuleId());

    assertEquals(
        CombinationBlockingDetails.builder()
            .blockingDetailsOperands(
                List.of(
                    IpBlockingDetails.builder().ipAddress("1.2.3.4").build(),
                    CustomSignatureBlockingDetails.builder().ruleId("rule-id-5").build()))
            .operator(Operator.AND)
            .build(),
        customSignatureRuleList.get(3).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_SIGNATURE_RULE,
        customSignatureRuleList.get(3).getCategory());
    assertEquals(
        BlockingPolicyDataBucket.CUSTOM_SIGNATURE_ANALYTICS,
        customSignatureRuleList.get(3).getBucket());
    assertEquals(RuleType.ANALYTICS, customSignatureRuleList.get(3).getRuleType());
    assertEquals("rule-id-5", customSignatureRuleList.get(3).getRuleId());
    assertNotNull(customSignatureRuleList.get(3).getAction());
    assertEquals(1, customSignatureRuleList.get(3).getAction().getInlineModificationsList().size());
    assertEquals(
        "header-name",
        customSignatureRuleList
            .get(3)
            .getAction()
            .getInlineModifications(0)
            .getHeaderInjection()
            .getHeaderName());
    assertEquals(
        "static-value",
        customSignatureRuleList
            .get(3)
            .getAction()
            .getInlineModifications(0)
            .getHeaderInjection()
            .getValue()
            .getStaticValue());
    assertEquals(activeTimestamp, customSignatureRuleList.get(3).getTimestamp());

    assertEquals(
        RegionBlockingDetails.builder().region("isoCode1").build(),
        customSignatureRuleList.get(4).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_SIGNATURE_RULE,
        customSignatureRuleList.get(4).getCategory());
    assertEquals(BlockingPolicyData.RuleType.BLOCK, customSignatureRuleList.get(4).getRuleType());
    assertEquals(BlockingPolicyData.Status.SUSPENDED, customSignatureRuleList.get(4).getStatus());
    assertNull(customSignatureRuleList.get(4).getAction());
    assertEquals(activeTimestamp, customSignatureRuleList.get(4).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomSignatureRuleViolationInfo(
            "rule-id-8", "rule-name-8", EVENT_SEVERITY_HIGH.name(), Map.of("key", "value")),
        customSignatureRuleList.get(4).getInfo());
    assertEquals("rule-id-8", customSignatureRuleList.get(4).getRuleId());
  }

  private static final List<CustomSignatureInlineRule> sampleCustomSignatureAllEnvRules =
      List.of(
          CustomSignatureInlineRule.newBuilder()
              .setRule(
                  CustomSignatureRule.newBuilder()
                      .setId("rule-id-1")
                      .setName("rule-name-1")
                      .setDescription("rule-description-1")
                      .setDefinition(
                          RuleDefinition.newBuilder()
                              .putAllLabels(Map.of("key", "value"))
                              .setClauseGroup(
                                  ClauseGroup.newBuilder().addClauses(matchClause).build())
                              .build())
                      .setEffect(
                          RuleEffect.newBuilder()
                              .setEventType(EVENT_TYPE_ALLOW)
                              .setEventSeverity(EVENT_SEVERITY_HIGH))
                      .setDisabled(false)
                      .setBlockingExpiryDetails(
                          ExpiryDetails.newBuilder().setExpiryTimestampMillis(activeTimestamp))
                      .build())
              .build(),
          CustomSignatureInlineRule.newBuilder()
              .setRule(
                  CustomSignatureRule.newBuilder()
                      .setId("rule-id-2")
                      .setName("rule-name-2")
                      .setDescription("rule-description-2")
                      .setDefinition(
                          RuleDefinition.newBuilder().putAllLabels(Map.of("key", "value")).build())
                      .setEffect(
                          RuleEffect.newBuilder()
                              .setEventType(EVENT_TYPE_ALLOW)
                              .setEventSeverity(EVENT_SEVERITY_HIGH))
                      .setDisabled(false)
                      .setBlockingExpiryDetails(
                          ExpiryDetails.newBuilder().setExpiryTimestampMillis(inactiveTimestamp))
                      .build())
              .build(),
          CustomSignatureInlineRule.newBuilder()
              .setRule(
                  CustomSignatureRule.newBuilder()
                      .setId("rule-id-3")
                      .setName("rule-name-3")
                      .setDescription("rule-description-3")
                      .setDefinition(
                          RuleDefinition.newBuilder()
                              .putAllLabels(Map.of("key", "value"))
                              .setClauseGroup(
                                  ClauseGroup.newBuilder().addClauses(ipAddressClause).build())
                              .build())
                      .setEffect(
                          RuleEffect.newBuilder()
                              .setEventType(EVENT_TYPE_DETECTION_AND_BLOCKING)
                              .setEventSeverity(EVENT_SEVERITY_HIGH))
                      .setDisabled(false)
                      .setBlockingExpiryDetails(
                          ExpiryDetails.newBuilder().setExpiryTimestampMillis(activeTimestamp))
                      .build())
              .build(),
          CustomSignatureInlineRule.newBuilder()
              .setRule(
                  CustomSignatureRule.newBuilder()
                      .setId("rule-id-4")
                      .setName("rule-name-4")
                      .setDescription("rule-description-4")
                      .setDefinition(
                          RuleDefinition.newBuilder().putAllLabels(Map.of("key", "value")).build())
                      .setEffect(
                          RuleEffect.newBuilder()
                              .setEventType(EVENT_TYPE_DETECTION_AND_BLOCKING)
                              .setEventSeverity(EVENT_SEVERITY_HIGH))
                      .setDisabled(false)
                      .setBlockingExpiryDetails(
                          ExpiryDetails.newBuilder().setExpiryTimestampMillis(inactiveTimestamp))
                      .build())
              .build(),
          CustomSignatureInlineRule.newBuilder()
              .setRule(
                  CustomSignatureRule.newBuilder()
                      .setId("rule-id-7")
                      .setName("rule-name-7")
                      .setDescription("rule-description-7")
                      .setDefinition(
                          RuleDefinition.newBuilder()
                              .putAllLabels(Map.of("key", "value"))
                              .setClauseGroup(
                                  ClauseGroup.newBuilder().addClauses(ipTypeClause).build())
                              .build())
                      .setEffect(
                          RuleEffect.newBuilder()
                              .setEventType(EVENT_TYPE_DETECTION_AND_BLOCKING)
                              .setEventSeverity(EVENT_SEVERITY_HIGH))
                      .setDisabled(false)
                      .setBlockingExpiryDetails(
                          ExpiryDetails.newBuilder().setExpiryTimestampMillis(activeTimestamp))
                      .build())
              .build());

  private static final List<CustomSignatureInlineRule> sampleCustomSignatureEnvRules =
      List.of(
          CustomSignatureInlineRule.newBuilder()
              .setRule(
                  CustomSignatureRule.newBuilder()
                      .setId("rule-id-5")
                      .setName("rule-name-5")
                      .setDescription("rule-description-5")
                      .setDefinition(
                          RuleDefinition.newBuilder()
                              .putAllLabels(Map.of("key", "value"))
                              .setClauseGroup(
                                  ClauseGroup.newBuilder()
                                      .addClauses(ipAddressClause)
                                      .addClauses(customSecRuleClause)
                                      .setClauseOperator(ClauseOperator.CLAUSE_OPERATOR_AND)
                                      .build())
                              .build())
                      .setEffect(
                          RuleEffect.newBuilder()
                              .setEventType(EVENT_TYPE_TESTING_DETECTION)
                              .setEventSeverity(EVENT_SEVERITY_HIGH)
                              .addEffects(
                                  RuleEffectWithModifications.newBuilder()
                                      .setAgentRuleEffect(
                                          AgentRuleEffect.newBuilder()
                                              .addAgentModifications(
                                                  AgentModification.newBuilder()
                                                      .setHeaderInjection(
                                                          HeaderInjection.newBuilder()
                                                              .setHeaderName("header-name")
                                                              .setHeaderCategory(
                                                                  MatchCategory
                                                                      .MATCH_CATEGORY_REQUEST)
                                                              .setValue(
                                                                  FieldValue.newBuilder()
                                                                      .setStaticValue(
                                                                          "static-value")))))))
                      .setRuleScope(
                          RuleScope.newBuilder()
                              .setEnvironmentScope(
                                  EnvironmentScope.newBuilder().addEnvironmentIds(ENVIRONMENT_ID)))
                      .setDisabled(false)
                      .setBlockingExpiryDetails(
                          ExpiryDetails.newBuilder().setExpiryTimestampMillis(activeTimestamp)))
              .build(),
          CustomSignatureInlineRule.newBuilder()
              .setRule(
                  CustomSignatureRule.newBuilder()
                      .setId("rule-id-6")
                      .setName("rule-name-6")
                      .setDescription("rule-description-6")
                      .setDefinition(
                          RuleDefinition.newBuilder().putAllLabels(Map.of("key", "value")).build())
                      .setEffect(
                          RuleEffect.newBuilder()
                              .setEventType(EVENT_TYPE_TESTING_DETECTION)
                              .setEventSeverity(EVENT_SEVERITY_HIGH))
                      .setRuleScope(
                          RuleScope.newBuilder()
                              .setEnvironmentScope(
                                  EnvironmentScope.newBuilder().addEnvironmentIds(ENVIRONMENT_ID)))
                      .setDisabled(false)
                      .setBlockingExpiryDetails(
                          ExpiryDetails.newBuilder().setExpiryTimestampMillis(activeTimestamp)))
              .build(),
          CustomSignatureInlineRule.newBuilder()
              .setRule(
                  CustomSignatureRule.newBuilder()
                      .setId("rule-id-8")
                      .setName("rule-name-8")
                      .setDescription("rule-description-8")
                      .setDefinition(
                          RuleDefinition.newBuilder()
                              .putAllLabels(Map.of("key", "value"))
                              .setClauseGroup(
                                  ClauseGroup.newBuilder().addClauses(regionClause).build())
                              .build())
                      .setEffect(
                          RuleEffect.newBuilder()
                              .setEventType(EVENT_TYPE_DETECTION_AND_BLOCKING)
                              .setEventSeverity(EVENT_SEVERITY_HIGH))
                      .setDisabled(false)
                      .setBlockingExpiryDetails(
                          ExpiryDetails.newBuilder().setExpiryTimestampMillis(activeTimestamp))
                      .build())
              .build());
}
