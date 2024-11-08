package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.customsignature.config.service.v1.EventSeverity.EVENT_SEVERITY_HIGH;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_ALLOW;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_DETECTION_AND_BLOCKING;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_TESTING_DETECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.blockingpolicy.BlockingPolicyDataBucket;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData.RuleType;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CustomSignatureBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.customsignature.config.service.v1.AgentModification;
import ai.traceable.customsignature.config.service.v1.AgentRuleEffect;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.FieldValue;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.HeaderInjection;
import ai.traceable.customsignature.config.service.v1.MatchCategory;
import ai.traceable.customsignature.config.service.v1.RuleDefinition;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleEffectWithModifications;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

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

  private CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub
      customSignatureConfigServiceBlockingStub;
  private CustomSignatureBlockingPolicyDataFetcher customSignatureDataFetcher;
  private static final long inactiveTimestamp = System.currentTimeMillis() - 10000L;
  private static final long activeTimestamp = System.currentTimeMillis() + 10000L;

  @BeforeEach
  void setUp() {
    customSignatureConfigServiceBlockingStub =
        mock(
            CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub.class,
            Answers.RETURNS_SELF);

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

    customSignatureDataFetcher =
        new CustomSignatureBlockingPolicyDataFetcher(
            customSignatureConfigServiceBlockingStub, blockingRulesUtils, ClientConfig.DEFAULT);
  }

  @Test
  void getCustomSignatureRulesTestEmpty() {
    doReturn(GetCustomSignatureRulesResponse.getDefaultInstance())
        .when(customSignatureConfigServiceBlockingStub)
        .getCustomSignatureRules(DEFAULT_GET_REQUEST);
    List<BlockingPolicyData> customSignatureRuleList =
        customSignatureDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList();
    assertEquals(0, customSignatureRuleList.size());
  }

  @Test
  void getCustomSignatureRulesTestWithoutEnvironment() {
    doReturn(sampleCustomSignatureAllEnvRulesResponse)
        .when(customSignatureConfigServiceBlockingStub)
        .getCustomSignatureRules(DEFAULT_GET_REQUEST);

    List<BlockingPolicyData> customSignatureRuleList =
        customSignatureDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList();

    assertEquals(2, customSignatureRuleList.size());

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
        CustomSignatureBlockingDetails.builder().ruleId("rule-id-3").build(),
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
  }

  @Test
  void getCustomSignatureRulesTestWithEnvironment() {
    doReturn(sampleCustomSignatureAllRulesResponse)
        .when(customSignatureConfigServiceBlockingStub)
        .getCustomSignatureRules(
            GetCustomSignatureRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .setRuleScope(
                            RuleScope.newBuilder()
                                .setEnvironmentScope(
                                    EnvironmentScope.newBuilder()
                                        .addEnvironmentIds(ENVIRONMENT_ID)))
                        .setDisabled(false))
                .build());
    List<BlockingPolicyData> customSignatureRuleList =
        customSignatureDataFetcher
            .getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder()
                    .environmentId(Optional.of(ENVIRONMENT_ID))
                    .build(),
                mock(BlockingRulesSupplier.class))
            .getBlockingPolicyList();
    assertEquals(3, customSignatureRuleList.size());
    assertEquals(
        CustomSignatureBlockingDetails.builder().ruleId("rule-id-1").build(),
        customSignatureRuleList.get(0).getBlockingDetails());
    assertEquals(
        CustomSignatureBlockingDetails.builder().ruleId("rule-id-3").build(),
        customSignatureRuleList.get(1).getBlockingDetails());
    assertEquals(
        CustomSignatureBlockingDetails.builder().ruleId("rule-id-5").build(),
        customSignatureRuleList.get(2).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_SIGNATURE_RULE,
        customSignatureRuleList.get(2).getCategory());
    assertEquals(
        BlockingPolicyDataBucket.CUSTOM_SIGNATURE_ANALYTICS,
        customSignatureRuleList.get(2).getBucket());
    assertEquals(RuleType.ANALYTICS, customSignatureRuleList.get(2).getRuleType());
    assertEquals("rule-id-5", customSignatureRuleList.get(2).getRuleId());
    assertNotNull(customSignatureRuleList.get(2).getAction());
    assertEquals(1, customSignatureRuleList.get(2).getAction().getInlineModificationsList().size());
    assertEquals(
        "header-name",
        customSignatureRuleList
            .get(2)
            .getAction()
            .getInlineModifications(0)
            .getHeaderInjection()
            .getHeaderName());
    assertEquals(
        "static-value",
        customSignatureRuleList
            .get(2)
            .getAction()
            .getInlineModifications(0)
            .getHeaderInjection()
            .getValue()
            .getStaticValue());
    assertEquals(activeTimestamp, customSignatureRuleList.get(2).getTimestamp());
    assertThrows(
        NullPointerException.class,
        () ->
            customSignatureDataFetcher.getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder().environmentId(Optional.empty()).build(),
                mock(BlockingRulesSupplier.class)));

    // Wrong environment
    assertThrows(
        NullPointerException.class,
        () ->
            customSignatureDataFetcher.getBlockingPolicyData(
                REQUEST_CONTEXT,
                BlockingPolicyDataFilter.builder()
                    .environmentId(Optional.of(ENVIRONMENT_ID + "random"))
                    .build(),
                mock(BlockingRulesSupplier.class)));
  }

  private static final GetCustomSignatureRulesResponse sampleCustomSignatureAllEnvRulesResponse =
      GetCustomSignatureRulesResponse.newBuilder()
          .addRules(
              CustomSignatureRule.newBuilder()
                  .setId("rule-id-1")
                  .setName("rule-name-1")
                  .setDescription("rule-description-1")
                  .setDefinition(
                      RuleDefinition.newBuilder().putAllLabels(Map.of("key", "value")).build())
                  .setEffect(
                      RuleEffect.newBuilder()
                          .setEventType(EVENT_TYPE_ALLOW)
                          .setEventSeverity(EVENT_SEVERITY_HIGH))
                  .setDisabled(false)
                  .setBlockingExpiryDetails(
                      ExpiryDetails.newBuilder().setExpiryTimestampMillis(activeTimestamp))
                  .build())
          .addRules(
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
          .addRules(
              CustomSignatureRule.newBuilder()
                  .setId("rule-id-3")
                  .setName("rule-name-3")
                  .setDescription("rule-description-3")
                  .setDefinition(
                      RuleDefinition.newBuilder().putAllLabels(Map.of("key", "value")).build())
                  .setEffect(
                      RuleEffect.newBuilder()
                          .setEventType(EVENT_TYPE_DETECTION_AND_BLOCKING)
                          .setEventSeverity(EVENT_SEVERITY_HIGH))
                  .setDisabled(false)
                  .setBlockingExpiryDetails(
                      ExpiryDetails.newBuilder().setExpiryTimestampMillis(activeTimestamp))
                  .build())
          .addRules(
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
                      ExpiryDetails.newBuilder().setExpiryTimestampMillis(inactiveTimestamp)))
          .build();
  private static final GetCustomSignatureRulesResponse sampleCustomSignatureAllRulesResponse =
      sampleCustomSignatureAllEnvRulesResponse.toBuilder()
          .addRules(
              CustomSignatureRule.newBuilder()
                  .setId("rule-id-5")
                  .setName("rule-name-5")
                  .setDescription("rule-description-5")
                  .setDefinition(
                      RuleDefinition.newBuilder().putAllLabels(Map.of("key", "value")).build())
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
                                                              MatchCategory.MATCH_CATEGORY_REQUEST)
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
          .addRules(
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
          .build();
}
