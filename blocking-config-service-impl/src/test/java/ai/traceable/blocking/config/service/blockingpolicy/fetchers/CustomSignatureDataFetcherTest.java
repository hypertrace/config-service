package ai.traceable.blocking.config.service.blockingpolicy.fetchers;

import static ai.traceable.blocking.config.service.v1.BlockingCategory.BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_ALLOW;
import static ai.traceable.blocking.config.service.v1.BlockingRuleType.BLOCKING_RULE_TYPE_BLOCK;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_ALLOWED;
import static ai.traceable.blocking.config.service.v1.BlockingStatus.BLOCKING_STATUS_DENIED;
import static ai.traceable.customsignature.config.service.v1.EventSeverity.EVENT_SEVERITY_HIGH;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_ALLOW;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_DETECTION_AND_BLOCKING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.blockingpolicy.fetchers.utils.BlockingRulesUtils;
import ai.traceable.blocking.config.service.v1.BlockingDetails;
import ai.traceable.blocking.config.service.v1.BlockingRuleType;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.ExpiryDetails;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleEffect;
import ai.traceable.platform.opa.v1.exemption.ExemptionInfoEncoder;
import ai.traceable.platform.opa.v1.violation.ViolationInfoEncoder;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CustomSignatureDataFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private CustomSignatureConfigServiceBlockingStub customSignatureConfigServiceBlockingStub;
  private CustomSignatureDataFetcher customSignatureDataFetcher;

  private static final long inactiveTimestamp = System.currentTimeMillis() - 10000L;
  private static final long activeTimestamp = System.currentTimeMillis() + 10000L;

  @BeforeEach
  void setUp() {
    customSignatureConfigServiceBlockingStub = mock(CustomSignatureConfigServiceBlockingStub.class);

    BlockingRulesUtils blockingRulesUtils = mock(BlockingRulesUtils.class);
    doReturn(true).when(blockingRulesUtils).isRuleActive(activeTimestamp);
    doReturn(false).when(blockingRulesUtils).isRuleActive(inactiveTimestamp);
    doReturn(BLOCKING_STATUS_ALLOWED)
        .when(blockingRulesUtils)
        .generateBlockingStatus(activeTimestamp, BLOCKING_RULE_TYPE_ALLOW);
    doReturn(BLOCKING_STATUS_DENIED)
        .when(blockingRulesUtils)
        .generateBlockingStatus(activeTimestamp, BLOCKING_RULE_TYPE_BLOCK);

    customSignatureDataFetcher =
        new CustomSignatureDataFetcher(
            customSignatureConfigServiceBlockingStub, blockingRulesUtils);
  }

  @Test
  void getCustomSignatureRulesTestEmpty() {
    doReturn(GetCustomSignatureRulesResponse.getDefaultInstance())
        .when(customSignatureConfigServiceBlockingStub)
        .getCustomSignatureRules(
            GetCustomSignatureRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .addEventTypes(EVENT_TYPE_DETECTION_AND_BLOCKING)
                        .addEventTypes(EVENT_TYPE_ALLOW)
                        .setDisabled(false)
                        .build())
                .build());

    Map<BlockingRuleType, List<BlockingDetails>> customSignatureRuleMap =
        customSignatureDataFetcher.getCustomSignatureRules(REQUEST_CONTEXT);

    assertEquals(2, customSignatureRuleMap.size());
    assertEquals(0, customSignatureRuleMap.get(BLOCKING_RULE_TYPE_ALLOW).size());
    assertEquals(0, customSignatureRuleMap.get(BLOCKING_RULE_TYPE_BLOCK).size());
  }

  @Test
  void getCustomSignatureRulesTest() {
    doReturn(
            GetCustomSignatureRulesResponse.newBuilder()
                .addRules(
                    CustomSignatureRule.newBuilder()
                        .setId("rule-id-1")
                        .setName("rule-name-1")
                        .setDescription("rule-description-1")
                        .setEffect(
                            RuleEffect.newBuilder()
                                .setEventType(EVENT_TYPE_ALLOW)
                                .setEventSeverity(EVENT_SEVERITY_HIGH)
                                .build())
                        .setDisabled(false)
                        .setBlockingExpiryDetails(
                            ExpiryDetails.newBuilder()
                                .setExpiryTimestampMillis(activeTimestamp)
                                .build())
                        .build())
                .addRules(
                    CustomSignatureRule.newBuilder()
                        .setId("rule-id-2")
                        .setName("rule-name-2")
                        .setDescription("rule-description-2")
                        .setEffect(
                            RuleEffect.newBuilder()
                                .setEventType(EVENT_TYPE_ALLOW)
                                .setEventSeverity(EVENT_SEVERITY_HIGH)
                                .build())
                        .setDisabled(false)
                        .setBlockingExpiryDetails(
                            ExpiryDetails.newBuilder()
                                .setExpiryTimestampMillis(inactiveTimestamp)
                                .build())
                        .build())
                .addRules(
                    CustomSignatureRule.newBuilder()
                        .setId("rule-id-3")
                        .setName("rule-name-3")
                        .setDescription("rule-description-3")
                        .setEffect(
                            RuleEffect.newBuilder()
                                .setEventType(EVENT_TYPE_DETECTION_AND_BLOCKING)
                                .setEventSeverity(EVENT_SEVERITY_HIGH)
                                .build())
                        .setDisabled(false)
                        .setBlockingExpiryDetails(
                            ExpiryDetails.newBuilder()
                                .setExpiryTimestampMillis(activeTimestamp)
                                .build())
                        .build())
                .addRules(
                    CustomSignatureRule.newBuilder()
                        .setId("rule-id-4")
                        .setName("rule-name-4")
                        .setDescription("rule-description-4")
                        .setEffect(
                            RuleEffect.newBuilder()
                                .setEventType(EVENT_TYPE_DETECTION_AND_BLOCKING)
                                .setEventSeverity(EVENT_SEVERITY_HIGH)
                                .build())
                        .setDisabled(false)
                        .setBlockingExpiryDetails(
                            ExpiryDetails.newBuilder()
                                .setExpiryTimestampMillis(inactiveTimestamp)
                                .build())
                        .build())
                .build())
        .when(customSignatureConfigServiceBlockingStub)
        .getCustomSignatureRules(
            GetCustomSignatureRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .addEventTypes(EVENT_TYPE_DETECTION_AND_BLOCKING)
                        .addEventTypes(EVENT_TYPE_ALLOW)
                        .setDisabled(false)
                        .build())
                .build());

    Map<BlockingRuleType, List<BlockingDetails>> customSignatureRuleMap =
        customSignatureDataFetcher.getCustomSignatureRules(REQUEST_CONTEXT);

    assertEquals(2, customSignatureRuleMap.size());

    List<BlockingDetails> exemptions = customSignatureRuleMap.get(BLOCKING_RULE_TYPE_ALLOW);
    assertEquals(1, exemptions.size());
    assertEquals("rule-id-1", exemptions.get(0).getCustomSignatureDetails().getRuleId());
    assertEquals(BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE, exemptions.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_ALLOW, exemptions.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_ALLOWED, exemptions.get(0).getStatus());
    assertEquals(
        ExemptionInfoEncoder.getEncodedCustomSignatureRuleExemptionInfo(
            "rule-id-1", "rule-name-1", EVENT_SEVERITY_HIGH.name()),
        exemptions.get(0).getInfo());

    List<BlockingDetails> violations = customSignatureRuleMap.get(BLOCKING_RULE_TYPE_BLOCK);
    assertEquals(1, violations.size());
    assertEquals("rule-id-3", violations.get(0).getCustomSignatureDetails().getRuleId());
    assertEquals(BLOCKING_CATEGORY_CUSTOM_SIGNATURE_RULE, violations.get(0).getCategory());
    assertEquals(BLOCKING_RULE_TYPE_BLOCK, violations.get(0).getBlockingRuleType());
    assertEquals(BLOCKING_STATUS_DENIED, violations.get(0).getStatus());
    assertEquals(
        ViolationInfoEncoder.getEncodedCustomSignatureRuleViolationInfo(
            "rule-id-3", "rule-name-3", EVENT_SEVERITY_HIGH.name()),
        violations.get(0).getInfo());
  }
}
