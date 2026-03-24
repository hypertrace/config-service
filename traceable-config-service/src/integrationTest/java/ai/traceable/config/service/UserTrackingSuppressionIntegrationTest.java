package ai.traceable.config.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.audit.utils.UserVisibleEmailConfig;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionAuditHelper;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesStore;
import com.google.protobuf.Value;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression test for AAP-11627: Verifies that config change events are NOT generated when
 * RequestContext has user tracking suppressed (e.g., during migrations).
 *
 * <p>This test exercises the hypertrace IdentifiedObjectStore's isUserTrackingSuppressed() guard.
 * If the hypertrace-config-service submodule is reverted to a version before commit 2105589
 * ("AAP-11113: Support for user-tracking suppression"), these tests will FAIL because the guard
 * will be missing and events will be unconditionally published.
 */
public class UserTrackingSuppressionIntegrationTest {

  private MockGenericConfigService mockConfigService;
  private ConfigChangeEventGenerator mockEventGenerator;
  private DetectionExclusionRulesStore rulesStore;

  @BeforeEach
  void setUp() {
    mockConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockUpsertAll();
    mockConfigService.start();

    ConfigServiceGrpc.ConfigServiceBlockingStub stub =
        ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    mockEventGenerator = mock(ConfigChangeEventGenerator.class);

    FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);
    when(featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(any())).thenReturn(false);

    DetectionExclusionConfigServiceConfig config =
        mock(DetectionExclusionConfigServiceConfig.class);
    when(config.getDefaultDetectionExclusionRules()).thenReturn(List.of());
    when(config.getDefaultNewDetectionExclusionRules()).thenReturn(List.of());
    when(config.getUserVisibleEmailConfig())
        .thenReturn(
            new UserVisibleEmailConfig(
                ConfigFactory.parseString(
                    "generic.config.service.customer.visible.excluded.email.patterns: []")));

    rulesStore =
        new DetectionExclusionRulesStore(
            stub,
            mockEventGenerator,
            featureCachingClient,
            config,
            new DetectionExclusionAuditHelper(config));
  }

  @AfterEach
  void tearDown() {
    mockConfigService.shutdown();
  }

  @Test
  void upsertObject_withSuppressedContext_shouldNotGenerateCreateEvent() {
    RequestContext suppressedContext =
        RequestContext.forTenantId("suppression-test-tenant").withUserTrackingSuppressed();
    assertTrue(suppressedContext.isUserTrackingSuppressed());

    DetectionExclusionRule rule = buildTestRule("suppressed-create-id");
    rulesStore.upsertObject(suppressedContext, rule);

    verify(mockEventGenerator, never())
        .sendCreateNotification(
            any(RequestContext.class), any(String.class), any(String.class), any(Value.class));
    verify(mockEventGenerator, never())
        .sendUpdateNotification(
            any(RequestContext.class),
            any(String.class),
            any(String.class),
            any(Value.class),
            any(Value.class));
  }

  @Test
  void upsertObject_withSuppressedContext_shouldNotGenerateUpdateEvent() {
    RequestContext normalContext = RequestContext.forTenantId("suppression-test-tenant");
    RequestContext suppressedContext = normalContext.withUserTrackingSuppressed();

    DetectionExclusionRule rule = buildTestRule("suppressed-update-id");

    // First upsert with normal context to create the record
    rulesStore.upsertObject(normalContext, rule);
    reset(mockEventGenerator);

    // Second upsert with suppressed context — should NOT generate update event
    DetectionExclusionRule updatedRule =
        rule.toBuilder()
            .setRuleInfo(rule.getRuleInfo().toBuilder().setName("updated-name"))
            .build();
    rulesStore.upsertObject(suppressedContext, updatedRule);

    verify(mockEventGenerator, never())
        .sendUpdateNotification(
            any(RequestContext.class),
            any(String.class),
            any(String.class),
            any(Value.class),
            any(Value.class));
    verify(mockEventGenerator, never())
        .sendCreateNotification(
            any(RequestContext.class), any(String.class), any(String.class), any(Value.class));
  }

  @Test
  void upsertObjects_withSuppressedContext_shouldNotGenerateEvents() {
    RequestContext suppressedContext =
        RequestContext.forTenantId("suppression-test-tenant").withUserTrackingSuppressed();

    List<DetectionExclusionRule> rules =
        List.of(buildTestRule("bulk-suppressed-1"), buildTestRule("bulk-suppressed-2"));

    rulesStore.upsertObjects(suppressedContext, rules);

    verify(mockEventGenerator, never())
        .sendCreateNotification(
            any(RequestContext.class), any(String.class), any(String.class), any(Value.class));
    verify(mockEventGenerator, never())
        .sendUpdateNotification(
            any(RequestContext.class),
            any(String.class),
            any(String.class),
            any(Value.class),
            any(Value.class));
  }

  @Test
  void deleteObject_withSuppressedContext_shouldNotGenerateDeleteEvent() {
    RequestContext normalContext = RequestContext.forTenantId("suppression-test-tenant");
    RequestContext suppressedContext = normalContext.withUserTrackingSuppressed();

    // Create a rule first
    DetectionExclusionRule rule = buildTestRule("suppressed-delete-id");
    rulesStore.upsertObject(normalContext, rule);
    reset(mockEventGenerator);

    // Delete with suppressed context — should NOT generate delete event
    rulesStore.deleteObject(suppressedContext, rule.getId());

    verify(mockEventGenerator, never())
        .sendDeleteNotification(
            any(RequestContext.class), any(String.class), any(String.class), any(Value.class));
  }

  @Test
  void upsertObject_withNormalContext_shouldGenerateEvents() {
    RequestContext normalContext = RequestContext.forTenantId("normal-test-tenant");
    assertFalse(normalContext.isUserTrackingSuppressed());

    DetectionExclusionRule rule = buildTestRule("normal-create-id");
    rulesStore.upsertObject(normalContext, rule);

    // Normal context SHOULD generate a create event
    verify(mockEventGenerator)
        .sendCreateNotification(
            any(RequestContext.class), any(String.class), any(String.class), any(Value.class));
  }

  private DetectionExclusionRule buildTestRule(String id) {
    return DetectionExclusionRule.newBuilder()
        .setId(id)
        .setRuleInfo(
            DetectionExclusionRuleInfo.newBuilder()
                .setName("test-rule-" + id)
                .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_ALERT)
                .setRuleStatus(
                    DetectionExclusionRuleStatus.newBuilder()
                        .setRuleCreationSource(RuleSource.RULE_SOURCE_CUSTOMER)))
        .setRuleScope(DetectionExclusionRuleScope.getDefaultInstance())
        .build();
  }
}
