package ai.traceable.blocking.config.service.common.blockingpolicy.fetchers;

import static ai.traceable.customsignature.config.service.v1.EventSeverity.EVENT_SEVERITY_HIGH;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_ALLOW;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_DETECTION_AND_BLOCKING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.blockingpolicy.data.BlockingPolicyData;
import ai.traceable.blocking.config.service.common.blockingpolicy.data.CustomSignatureBlockingDetails;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import java.util.List;
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
                  .addEventTypes(EVENT_TYPE_DETECTION_AND_BLOCKING)
                  .addEventTypes(EVENT_TYPE_ALLOW)
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
        mock(CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub.class);

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
            customSignatureConfigServiceBlockingStub, blockingRulesUtils);
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

    assertEquals(
        CustomSignatureBlockingDetails.builder().ruleId("rule-id-3").build(),
        customSignatureRuleList.get(1).getBlockingDetails());
    assertEquals(
        BlockingPolicyData.Category.CUSTOM_SIGNATURE_RULE,
        customSignatureRuleList.get(1).getCategory());
    assertEquals(BlockingPolicyData.RuleType.BLOCK, customSignatureRuleList.get(1).getRuleType());
    assertEquals(BlockingPolicyData.Status.SUSPENDED, customSignatureRuleList.get(1).getStatus());
    assertEquals(activeTimestamp, customSignatureRuleList.get(1).getTimestamp());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomSignatureRuleViolationInfo(
            "rule-id-3", "rule-name-3", EVENT_SEVERITY_HIGH.name()),
        customSignatureRuleList.get(1).getInfo());
  }

  @Test
  void getCustomSignatureRulesTestWithEnvironment() {
    doReturn(sampleCustomSignatureAllRulesResponse)
        .when(customSignatureConfigServiceBlockingStub)
        .getCustomSignatureRules(
            GetCustomSignatureRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .addEventTypes(EVENT_TYPE_DETECTION_AND_BLOCKING)
                        .addEventTypes(EVENT_TYPE_ALLOW)
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
                  .setEffect(
                      RuleEffect.newBuilder()
                          .setEventType(EVENT_TYPE_ALLOW)
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
