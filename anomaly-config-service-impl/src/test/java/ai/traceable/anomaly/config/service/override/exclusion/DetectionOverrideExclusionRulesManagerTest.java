package ai.traceable.anomaly.config.service.override.exclusion;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.v1.override.CreateDetectionExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.override.DetectionExclusionConfigCriteria;
import ai.traceable.anomaly.config.service.v1.override.DetectionExclusionRule;
import ai.traceable.anomaly.config.service.v1.override.DetectionExclusionRuleInfo;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideRuleScope;
import ai.traceable.anomaly.config.service.v1.override.GetDetectionExclusionRulesFilter;
import ai.traceable.config.utils.UuidGenerator;
import java.util.List;
import java.util.NoSuchElementException;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DetectionOverrideExclusionRulesManagerTest {

  private MockGenericConfigService mockConfigService;
  private UuidGenerator uuidGenerator;
  private DetectionOverrideRulesStore rulesStore;
  private DetectionOverrideExclusionRulesManager rulesManager;
  private RequestContext requestContext;

  @BeforeEach
  public void setup() {
    mockConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfigService.start();
    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    rulesStore =
        new DetectionOverrideRulesStore(
            configServiceBlockingStub, mock(ConfigChangeEventGenerator.class));
    uuidGenerator = mock(UuidGenerator.class);
    rulesManager = new DetectionOverrideExclusionRulesManager(rulesStore, uuidGenerator);
    requestContext = RequestContext.forTenantId("test-tenant");
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  @Test
  public void testGetDetectionExclusionRules() {
    // create rules
    DetectionExclusionRule rule1 =
        DetectionExclusionRule.newBuilder()
            .setId("id1")
            .setRuleScope(
                DetectionOverrideRuleScope.newBuilder()
                    .setEnvironmentScope(
                        DetectionOverrideEnvironmentScope.newBuilder()
                            .addAllEnvironmentIds(List.of("dev"))))
            .build();
    rulesStore.upsertObject(requestContext, rule1);
    DetectionExclusionRule rule2 =
        DetectionExclusionRule.newBuilder()
            .setId("id2")
            .setRuleScope(
                DetectionOverrideRuleScope.newBuilder()
                    .setEnvironmentScope(
                        DetectionOverrideEnvironmentScope.newBuilder()
                            .addAllEnvironmentIds(List.of("dev", "prod"))))
            .build();
    rulesStore.upsertObject(requestContext, rule2);
    DetectionExclusionRule rule3 = DetectionExclusionRule.newBuilder().setId("id3").build();
    rulesStore.upsertObject(requestContext, rule3);

    // no filter
    List<DetectionExclusionRule> rules =
        rulesManager.getDetectionExclusionRules(
            requestContext, GetDetectionExclusionRulesFilter.getDefaultInstance());
    assertEquals(List.of(rule3, rule2, rule1), rules);

    // all environment rules filter
    GetDetectionExclusionRulesFilter filter =
        GetDetectionExclusionRulesFilter.newBuilder()
            .setRuleScope(
                DetectionOverrideRuleScope.newBuilder()
                    .setEnvironmentScope(
                        DetectionOverrideEnvironmentScope.newBuilder()
                            .addAllEnvironmentIds(List.of())))
            .build();
    rules = rulesManager.getDetectionExclusionRules(requestContext, filter);
    assertEquals(List.of(rule3), rules);

    // environment filter
    filter =
        GetDetectionExclusionRulesFilter.newBuilder()
            .setRuleScope(
                DetectionOverrideRuleScope.newBuilder()
                    .setEnvironmentScope(
                        DetectionOverrideEnvironmentScope.newBuilder()
                            .addAllEnvironmentIds(List.of("dev"))))
            .build();
    rules = rulesManager.getDetectionExclusionRules(requestContext, filter);
    assertEquals(List.of(rule3, rule2, rule1), rules);

    filter =
        GetDetectionExclusionRulesFilter.newBuilder()
            .setRuleScope(
                DetectionOverrideRuleScope.newBuilder()
                    .setEnvironmentScope(
                        DetectionOverrideEnvironmentScope.newBuilder()
                            .addAllEnvironmentIds(List.of("staging"))))
            .build();
    rules = rulesManager.getDetectionExclusionRules(requestContext, filter);
    assertEquals(List.of(rule3), rules);

    filter =
        GetDetectionExclusionRulesFilter.newBuilder()
            .setRuleScope(
                DetectionOverrideRuleScope.newBuilder()
                    .setEnvironmentScope(
                        DetectionOverrideEnvironmentScope.newBuilder()
                            .addAllEnvironmentIds(List.of("prod"))))
            .build();
    rules = rulesManager.getDetectionExclusionRules(requestContext, filter);
    assertEquals(List.of(rule3, rule2), rules);
  }

  @Test
  public void testCreateDetectionExclusionRule() {
    DetectionOverrideRuleScope ruleScope =
        DetectionOverrideRuleScope.newBuilder()
            .setEnvironmentScope(
                DetectionOverrideEnvironmentScope.newBuilder()
                    .addAllEnvironmentIds(List.of("prod")))
            .build();
    DetectionExclusionRuleInfo ruleInfo =
        DetectionExclusionRuleInfo.newBuilder()
            .setName("test_rule_info")
            .setDescription("test_rule_info")
            .setCriteria(DetectionExclusionConfigCriteria.getDefaultInstance())
            .build();

    DetectionExclusionRule expectedRule =
        DetectionExclusionRule.newBuilder()
            .setId("test-id")
            .setRuleInfo(ruleInfo)
            .setRuleScope(ruleScope)
            .build();

    when(uuidGenerator.generateRandomId()).thenReturn("test-id");
    DetectionExclusionRule actualRule =
        rulesManager.createDetectionExclusionRule(
            requestContext,
            CreateDetectionExclusionRuleRequest.newBuilder()
                .setRuleInfo(ruleInfo)
                .setRuleScope(ruleScope)
                .build());

    assertEquals(expectedRule, actualRule);
  }

  @Test
  public void testUpdateDetectionExclusionRule() {
    DetectionExclusionRule originalRule =
        DetectionExclusionRule.newBuilder()
            .setId("id")
            .setRuleScope(
                DetectionOverrideRuleScope.newBuilder()
                    .setEnvironmentScope(
                        DetectionOverrideEnvironmentScope.newBuilder()
                            .addAllEnvironmentIds(List.of("dev"))))
            .build();

    DetectionExclusionRule updatedRule =
        DetectionExclusionRule.newBuilder()
            .setId("id")
            .setRuleScope(
                DetectionOverrideRuleScope.newBuilder()
                    .setEnvironmentScope(
                        DetectionOverrideEnvironmentScope.newBuilder()
                            .addAllEnvironmentIds(List.of("prod"))))
            .build();

    assertThrows(
        NoSuchElementException.class,
        () -> rulesManager.updateDetectionExclusionRule(requestContext, updatedRule));

    rulesStore.upsertObject(requestContext, originalRule);
    assertEquals(
        updatedRule, rulesManager.updateDetectionExclusionRule(requestContext, updatedRule));
  }

  @Test
  public void testDeleteDetectionExclusionRule() {
    assertThrows(
        RuntimeException.class,
        () -> rulesManager.deleteDetectionExclusionRule(requestContext, "id1"));
    rulesStore.upsertObject(
        requestContext, DetectionExclusionRule.newBuilder().setId("id1").build());
    assertDoesNotThrow(() -> rulesManager.deleteDetectionExclusionRule(requestContext, "id1"));
  }
}
