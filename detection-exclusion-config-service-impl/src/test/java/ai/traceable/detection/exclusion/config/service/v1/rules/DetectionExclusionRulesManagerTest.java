package ai.traceable.detection.exclusion.config.service.v1.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DetectionExclusionRulesManagerTest {

  private UuidGenerator uuidGenerator;
  private DetectionExclusionRulesManager rulesManager;
  private final RequestContext requestContext = RequestContext.forTenantId("tenantId");

  @BeforeEach
  void setUp() {
    MockGenericConfigService mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    ConfigChangeEventGenerator mockConfigChangeEventGenerator =
        mock(ConfigChangeEventGenerator.class);
    DetectionExclusionRulesStore rulesStore =
        new DetectionExclusionRulesStore(configServiceBlockingStub, mockConfigChangeEventGenerator);
    uuidGenerator = mock(UuidGenerator.class);
    rulesManager = new DetectionExclusionRulesManager(rulesStore, uuidGenerator);
  }

  @Test
  void testCRUDDetectionExclusionRules() {
    DetectionExclusionRuleInfo detectionExclusionRuleInfo =
        DetectionExclusionRuleInfo.newBuilder().setName("rule").build();
    DetectionExclusionRuleScope detectionExclusionRuleScope =
        DetectionExclusionRuleScope.getDefaultInstance();
    DetectionExclusionRule detectionExclusionRule =
        DetectionExclusionRule.newBuilder()
            .setId("id")
            .setRuleInfo(detectionExclusionRuleInfo)
            .setRuleScope(detectionExclusionRuleScope)
            .build();

    when(uuidGenerator.generateRandomId()).thenReturn("id");

    // creating detection exclusion rule
    assertEquals(
        detectionExclusionRule,
        rulesManager.createDetectionExclusionRule(
            requestContext, detectionExclusionRuleScope, detectionExclusionRuleInfo));

    // fetching detection exclusion rule
    assertEquals(
        List.of(detectionExclusionRule),
        rulesManager.getDetectionExclusionRules(
            requestContext, GetRulesFilter.getDefaultInstance()));
    assertEquals(
        List.of(detectionExclusionRule),
        rulesManager.getDetectionExclusionRules(
            requestContext, GetRulesFilter.newBuilder().setDisabled(false).build()));

    detectionExclusionRule =
        DetectionExclusionRule.newBuilder()
            .setId("id")
            .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("rule1"))
            .setRuleScope(detectionExclusionRuleScope)
            .build();

    // updating detection exclusion rule
    assertEquals(
        detectionExclusionRule,
        rulesManager.updateDetectionExclusionRule(requestContext, detectionExclusionRule));

    // deleting detection exclusion rule
    assertEquals(
        detectionExclusionRule, rulesManager.deleteDetectionExclusionRule(requestContext, "id"));

    // no rules after deletion
    assertEquals(
        List.of(),
        rulesManager.getDetectionExclusionRules(
            requestContext, GetRulesFilter.getDefaultInstance()));
  }
}
