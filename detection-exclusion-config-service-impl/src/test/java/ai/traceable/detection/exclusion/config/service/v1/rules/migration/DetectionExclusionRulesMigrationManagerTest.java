package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import static ai.traceable.detection.exclusion.config.service.v1.rules.migration.DetectionExclusionRulesMigrationManager.OLD_RULES_FILTER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.exclusion.handlers.AnomalyExclusionRuleConfigStore;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionType;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionMigrationConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.EntityScope;
import ai.traceable.detection.exclusion.config.service.v1.EntityType;
import ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope;
import ai.traceable.detection.exclusion.config.service.v1.EventCondition;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import ai.traceable.detection.exclusion.config.service.v1.ScopeCondition;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesStore;
import ai.traceable.platform.config.provider.common.clients.ActorServiceClient;
import com.typesafe.config.ConfigFactory;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DetectionExclusionRulesMigrationManagerTest {

  private final RequestContext requestContext = RequestContext.forTenantId("tenantId");
  private final AnomalyExclusionRuleConfig sampleOldRuleConfig = getSampleOldRuleConfig();
  private final DetectionExclusionRule sampleNewRule = getSampleNewRule();
  private final Instant creationTimestamp = Instant.now();

  private final List<ContextualConfigObject<DetectionExclusionRule>> newRules =
      List.of(
          getNewRuleContextualConfigObject("id1", creationTimestamp.plusSeconds(10)),
          getNewRuleContextualConfigObject("id2", creationTimestamp.plusSeconds(20)),
          getNewRuleContextualConfigObject("id3", creationTimestamp.plusSeconds(30)));

  private final List<ContextualConfigObject<AnomalyExclusionRuleConfig>> oldRules =
      List.of(
          getOldRuleContextualConfigObject("id0", creationTimestamp),
          getOldRuleContextualConfigObject("id1", creationTimestamp.plusSeconds(5)),
          getOldRuleContextualConfigObject("id2", creationTimestamp.plusSeconds(25)));

  private FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);
  private DetectionExclusionRulesStore newRulesStore = mock(DetectionExclusionRulesStore.class);
  private AnomalyExclusionRuleConfigStore oldRulesStore =
      mock(AnomalyExclusionRuleConfigStore.class);
  private DetectionExclusionMigrationStore migrationStore =
      mock(DetectionExclusionMigrationStore.class);
  private DetectionExclusionRulesMigrationManager migrationManager;

  @BeforeEach
  void setup() {
    featureCachingClient = mock(FeatureCachingClient.class);
    newRulesStore = mock(DetectionExclusionRulesStore.class);
    oldRulesStore = mock(AnomalyExclusionRuleConfigStore.class);
    migrationStore = mock(DetectionExclusionMigrationStore.class);
    ActorServiceClient actorServiceClient = mock(ActorServiceClient.class);
    when(actorServiceClient.getActorsByEntityIds(any(), any())).thenReturn(List.of());

    migrationManager =
        new DetectionExclusionRulesMigrationManager(
            featureCachingClient,
            newRulesStore,
            oldRulesStore,
            migrationStore,
            new DetectionExclusionRuleConverter(),
            actorServiceClient,
            new DetectionExclusionConfigServiceConfig(ConfigFactory.empty()),
            new DetectionExclusionRuleEvaluationPointsMigrator());
  }

  @Test
  void testMigration_oldRules() {
    when(featureCachingClient.isDetectionExclusionV2EnabledForTenant(any(RequestContext.class)))
        .thenReturn(true);
    when(newRulesStore.getAllObjects(any(), any())).thenReturn(newRules);
    when(oldRulesStore.getAllObjects(any())).thenReturn(oldRules);
    DetectionExclusionMigrationConfig completedMigrationConfig =
        mockMigrationStore(false, false, false, false).toBuilder()
            .setMigrationCompleted(true)
            .build();

    migrationManager.migrateFromOldStoreIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(1))
        .upsertObject(any(RequestContext.class), eq(completedMigrationConfig));
    verify(oldRulesStore, times(1)).getAllObjects(any(RequestContext.class));
    verify(newRulesStore, times(1)).getAllObjects(any(RequestContext.class), eq(OLD_RULES_FILTER));
    verify(newRulesStore, times(2)).upsertObjects(any(RequestContext.class), any());
    verify(newRulesStore, times(1))
        .upsertObjects(
            any(RequestContext.class),
            argThat(list -> list.size() == 1 && list.get(0).getId().equals("id0")));
    verify(newRulesStore, times(1))
        .upsertObjects(
            any(RequestContext.class),
            argThat(list -> list.size() == 1 && list.get(0).getId().equals("id2")));

    resetStores();
    migrationManager.migrateFromOldStoreIfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  @Test
  void testMigration_noUpdate_noOldRules() {
    when(featureCachingClient.isDetectionExclusionV2EnabledForTenant(any(RequestContext.class)))
        .thenReturn(true);
    when(newRulesStore.getAllObjects(any(), any())).thenReturn(Collections.emptyList());
    when(oldRulesStore.getAllObjects(any())).thenReturn(Collections.emptyList());
    DetectionExclusionMigrationConfig completedMigrationConfig =
        mockMigrationStore(false, false, false, false).toBuilder()
            .setMigrationCompleted(true)
            .build();

    migrationManager.migrateFromOldStoreIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(1))
        .upsertObject(any(RequestContext.class), eq(completedMigrationConfig));
    verify(oldRulesStore, times(1)).getAllObjects(any(RequestContext.class));
    verify(newRulesStore, times(1)).getAllObjects(any(RequestContext.class), eq(OLD_RULES_FILTER));
    verify(newRulesStore, times(0)).upsertObjects(any(RequestContext.class), any());

    resetStores();
    migrationManager.migrateFromOldStoreIfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  @Test
  void testMigration_noUpdate_migrationCompleted() {
    when(featureCachingClient.isDetectionExclusionV2EnabledForTenant(any(RequestContext.class)))
        .thenReturn(true);
    when(newRulesStore.getAllObjects(any(), any())).thenReturn(newRules);
    when(oldRulesStore.getAllObjects(any())).thenReturn(oldRules);
    mockMigrationStore(true, false, false, false);

    migrationManager.migrateFromOldStoreIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(0)).upsertObject(any(RequestContext.class), any());
    verifyZeroInteractionWithRulesStore(false);

    resetStores();
    migrationManager.migrateFromOldStoreIfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  @Test
  void testMigration_changeLog2() {
    when(newRulesStore.getAllConfigData(any()))
        .thenReturn(
            List.of(
                sampleNewRule,
                getSampleChangeLog2Rule(false, "id2a"),
                getSampleChangeLog2Rule(true, "id2b")));
    DetectionExclusionMigrationConfig completedMigrationConfig =
        mockMigrationStore(true, false, false, false).toBuilder()
            .setChangeLog2MigrationCompleted(true)
            .build();

    migrationManager.migrateFromChangeLog2IfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(1))
        .upsertObject(any(RequestContext.class), eq(completedMigrationConfig));
    verify(newRulesStore, times(1)).getAllConfigData(any(RequestContext.class));
    verify(newRulesStore, times(1)).upsertObjects(any(RequestContext.class), any());
    verify(newRulesStore, times(1))
        .upsertObjects(
            any(RequestContext.class),
            argThat(
                list ->
                    list.size() == 2
                        && verifyExclusionTargets("id1", list.get(0))
                        && verifyExclusionTargets("id2a", list.get(1))));

    resetStores();
    migrationManager.migrateFromChangeLog2IfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  private boolean verifyExclusionTargets(String id, DetectionExclusionRule rule) {
    assertEquals(id, rule.getId());
    List<ExclusionTarget> targets = rule.getRuleInfo().getExclusionTargetsList();
    assertEquals(1, targets.size());
    assertTrue(targets.contains(ExclusionTarget.EXCLUSION_TARGET_ALERT));
    return true;
  }

  @Test
  void testMigrationCompleted_changeLog2() {
    when(newRulesStore.getAllConfigData(any()))
        .thenReturn(
            List.of(
                sampleNewRule,
                getSampleChangeLog2Rule(false, "id2a"),
                getSampleChangeLog2Rule(true, "id2b")));
    mockMigrationStore(false, true, false, false);

    migrationManager.migrateFromChangeLog2IfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(0)).upsertObject(any(RequestContext.class), any());
    verify(newRulesStore, times(0)).getAllConfigData(any(RequestContext.class));

    resetStores();
    migrationManager.migrateFromChangeLog2IfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  @Test
  void testMigration_changeLog3() {
    when(newRulesStore.getAllConfigData(any()))
        .thenReturn(
            List.of(
                sampleNewRule, getSampleSstiRule(false, "id3a"), getSampleSstiRule(true, "id3b")));
    DetectionExclusionMigrationConfig completedMigrationConfig =
        mockMigrationStore(true, false, false, false).toBuilder()
            .setChangeLog3MigrationCompleted(true)
            .build();

    migrationManager.migrateFromChangeLog3IfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(1))
        .upsertObject(any(RequestContext.class), eq(completedMigrationConfig));
    verify(newRulesStore, times(1)).getAllConfigData(any(RequestContext.class));
    verify(newRulesStore, times(1)).upsertObjects(any(RequestContext.class), any());
    verify(newRulesStore, times(1))
        .upsertObjects(
            any(RequestContext.class),
            argThat(list -> list.size() == 1 && verifySsti(list.get(0))));

    resetStores();
    migrationManager.migrateFromChangeLog3IfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  @Test
  void testMigration_changeLog4() {
    when(newRulesStore.getAllConfigData(any()))
        .thenReturn(
            List.of(
                sampleNewRule,
                getSampleSourceRule(false, "id3a"),
                getSampleSourceRule(true, "id3b")));
    DetectionExclusionMigrationConfig completedMigrationConfig =
        mockMigrationStore(true, true, true, false).toBuilder()
            .setChangeLog4MigrationCompleted(true)
            .build();

    migrationManager.migrateFromChangeLog4IfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(1))
        .upsertObject(any(RequestContext.class), eq(completedMigrationConfig));
    verify(newRulesStore, times(1)).getAllConfigData(any(RequestContext.class));
    verify(newRulesStore, times(1)).upsertObjects(any(RequestContext.class), any());
    verify(newRulesStore, times(1))
        .upsertObjects(
            any(RequestContext.class),
            argThat(list -> list.size() == 1 && verifyHidden(list.get(0))));

    resetStores();
    migrationManager.migrateFromChangeLog4IfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  private boolean verifyHidden(DetectionExclusionRule rule) {
    assertEquals("id3a", rule.getId());
    assertTrue(rule.getRuleInfo().getRuleStatus().getHidden());
    return true;
  }

  private boolean verifySsti(DetectionExclusionRule rule) {
    assertEquals("id3a", rule.getId());
    assertTrue(
        rule.getRuleInfo().getConditionsList().stream()
            .flatMap(
                condition -> condition.getEventCondition().getSystemDefinedEventsList().stream())
            .filter(
                event ->
                    event
                        .getEventFamily()
                        .equals(SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_MODSEC))
            .allMatch(event -> event.getEventSubTypeId().equals("crs_9320310")));
    return true;
  }

  @Test
  void testMigrationCompleted_changeLog3() {
    when(newRulesStore.getAllConfigData(any()))
        .thenReturn(
            List.of(
                sampleNewRule, getSampleSstiRule(false, "id3a"), getSampleSstiRule(true, "id3b")));
    mockMigrationStore(false, false, true, false);

    migrationManager.migrateFromChangeLog3IfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(0)).upsertObject(any(RequestContext.class), any());
    verify(newRulesStore, times(0)).getAllConfigData(any(RequestContext.class));

    resetStores();
    migrationManager.migrateFromChangeLog3IfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  @Test
  void testMigrationCompleted_changeLog4() {
    when(newRulesStore.getAllConfigData(any()))
        .thenReturn(
            List.of(
                sampleNewRule,
                getSampleSourceRule(false, "id3a"),
                getSampleSourceRule(true, "id3b")));
    mockMigrationStore(false, false, false, true);

    migrationManager.migrateFromChangeLog4IfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(0)).upsertObject(any(RequestContext.class), any());
    verify(newRulesStore, times(0)).getAllConfigData(any(RequestContext.class));

    resetStores();
    migrationManager.migrateFromChangeLog4IfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  @Test
  void testMigration_apiProtectionExclusionRules_ForwardMigration() {
    when(featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(any(RequestContext.class)))
        .thenReturn(true);
    when(newRulesStore.getAllConfigDataWithoutDefaults(any()))
        .thenReturn(
            List.of(
                getSampleApiProtectionRule(false, "id4a"),
                getSampleApiProtectionRule(true, "id4b")));
    DetectionExclusionMigrationConfig completedMigrationConfig =
        mockMigrationStore(true, true, true, true, false).toBuilder()
            .setApiProtectionExclusionRulesMigrationCompleted(true)
            .build();

    migrationManager.migrateForApiProtectionExclusionRulesIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(1))
        .upsertObject(any(RequestContext.class), eq(completedMigrationConfig));
    verify(newRulesStore, times(1)).getAllConfigDataWithoutDefaults(any(RequestContext.class));
    verify(newRulesStore, times(1)).upsertObjects(any(RequestContext.class), any());
    verify(newRulesStore, times(1))
        .upsertObjects(
            any(RequestContext.class),
            argThat(list -> list.size() == 1 && verifyApiProtectionForward(list.get(0))));

    resetStores();
    migrationManager.migrateForApiProtectionExclusionRulesIfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  @Test
  void testMigrationCompleted_apiProtectionExclusionRules() {
    when(featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(any(RequestContext.class)))
        .thenReturn(true);
    when(newRulesStore.getAllConfigDataWithoutDefaults(any()))
        .thenReturn(
            List.of(
                sampleNewRule,
                getSampleApiProtectionRule(false, "id4a"),
                getSampleApiProtectionRule(true, "id4b")));
    mockMigrationStore(false, false, false, false, true);

    migrationManager.migrateForApiProtectionExclusionRulesIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(0)).upsertObject(any(RequestContext.class), any());
    verify(newRulesStore, times(0)).getAllConfigDataWithoutDefaults(any(RequestContext.class));

    resetStores();
    migrationManager.migrateForApiProtectionExclusionRulesIfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  @Test
  void testMigration_apiProtectionExclusionRules_BackwardMigration() {
    // Feature flag is disabled, migration was completed -> trigger backward migration
    when(featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(any(RequestContext.class)))
        .thenReturn(false);
    when(newRulesStore.getAllConfigDataWithoutDefaults(any()))
        .thenReturn(
            List.of(
                getSampleApiProtectionRule(true, "id4a"), // Already migrated rule
                getSampleApiProtectionRule(false, "id4b"))); // Not migrated rule
    DetectionExclusionMigrationConfig rollbackMigrationConfig =
        mockMigrationStore(true, true, true, true, true).toBuilder()
            .setApiProtectionExclusionRulesMigrationCompleted(false)
            .build();

    migrationManager.migrateForApiProtectionExclusionRulesIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(1))
        .upsertObject(any(RequestContext.class), eq(rollbackMigrationConfig));
    verify(newRulesStore, times(1)).getAllConfigDataWithoutDefaults(any(RequestContext.class));
    verify(newRulesStore, times(1)).upsertObjects(any(RequestContext.class), any());
    verify(newRulesStore, times(1))
        .upsertObjects(
            any(RequestContext.class),
            argThat(list -> list.size() == 1 && verifyApiProtectionBackward(list.get(0))));
  }

  @Test
  void testMigration_apiProtectionExclusionRules_BackwardMigration_ContentType() {
    // Feature flag is disabled, migration was completed -> trigger backward migration
    when(featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(any(RequestContext.class)))
        .thenReturn(false);
    when(newRulesStore.getAllConfigDataWithoutDefaults(any()))
        .thenReturn(
            List.of(
                getSampleApiProtectionContentTypeRule(true, "id5a"), // Already migrated rule
                getSampleApiProtectionContentTypeRule(false, "id5b")));
    DetectionExclusionMigrationConfig rollbackMigrationConfig =
        mockMigrationStore(true, true, true, true, true).toBuilder()
            .setApiProtectionExclusionRulesMigrationCompleted(false)
            .build();

    migrationManager.migrateForApiProtectionExclusionRulesIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(1))
        .upsertObject(any(RequestContext.class), eq(rollbackMigrationConfig));
    verify(newRulesStore, times(1)).getAllConfigDataWithoutDefaults(any(RequestContext.class));
    verify(newRulesStore, times(1)).upsertObjects(any(RequestContext.class), any());
    verify(newRulesStore, times(1))
        .upsertObjects(
            any(RequestContext.class),
            argThat(list -> list.size() == 1 && verifyContentTypeBackward(list.get(0))));
  }

  @Test
  void testNoMigration_apiProtectionExclusionRules_FeatureFlagDisabled_NotCompleted() {
    // Feature flag is disabled, migration NOT completed -> do nothing
    when(featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(any(RequestContext.class)))
        .thenReturn(false);
    mockMigrationStore(true, true, true, true, false);

    migrationManager.migrateForApiProtectionExclusionRulesIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(0)).upsertObject(any(RequestContext.class), any());
    verify(newRulesStore, times(0)).getAllConfigDataWithoutDefaults(any(RequestContext.class));
  }

  @Test
  void testMigration_apiProtectionExclusionRules_FullLifecycle_FlagToggling() {
    // This test covers the full lifecycle: FF enabled -> FF disabled -> FF enabled again

    // ==================== PHASE 1: FF ENABLED - Forward Migration ====================
    when(featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(any(RequestContext.class)))
        .thenReturn(true);
    when(newRulesStore.getAllConfigDataWithoutDefaults(any()))
        .thenReturn(List.of(getSampleApiProtectionRule(false, "id4a")));

    DetectionExclusionMigrationConfig initialConfig =
        mockMigrationStore(true, true, true, true, false);
    DetectionExclusionMigrationConfig forwardMigrationConfig =
        initialConfig.toBuilder().setApiProtectionExclusionRulesMigrationCompleted(true).build();

    // First call: Forward migration should happen
    migrationManager.migrateForApiProtectionExclusionRulesIfApplicable(requestContext);

    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(1))
        .upsertObject(any(RequestContext.class), eq(forwardMigrationConfig));
    verify(newRulesStore, times(1)).getAllConfigDataWithoutDefaults(any(RequestContext.class));
    verify(newRulesStore, times(1))
        .upsertObjects(
            any(RequestContext.class),
            argThat(list -> list.size() == 1 && verifyApiProtectionForward(list.get(0))));

    // ==================== PHASE 2: FF STILL ENABLED - Cached (no work) ====================
    resetStores();
    when(featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(any(RequestContext.class)))
        .thenReturn(true);
    // No need to mock migrationStore since we return early before accessing it

    // Second call: Should return early (cached), no DB access at all
    migrationManager.migrateForApiProtectionExclusionRulesIfApplicable(requestContext);

    verify(migrationStore, times(0)).getData(any(RequestContext.class));
    verify(migrationStore, times(0)).upsertObject(any(RequestContext.class), any());
    verify(newRulesStore, times(0)).getAllConfigDataWithoutDefaults(any(RequestContext.class));

    // ==================== PHASE 3: FF DISABLED - Backward Migration ====================
    resetStores();
    when(featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(any(RequestContext.class)))
        .thenReturn(false);
    when(newRulesStore.getAllConfigDataWithoutDefaults(any()))
        .thenReturn(List.of(getSampleApiProtectionRule(true, "id4a"))); // Already migrated

    DetectionExclusionMigrationConfig backwardMigrationConfig =
        mockMigrationStore(true, true, true, true, true).toBuilder()
            .setApiProtectionExclusionRulesMigrationCompleted(false)
            .build();

    // Third call: Backward migration should happen
    migrationManager.migrateForApiProtectionExclusionRulesIfApplicable(requestContext);

    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(1))
        .upsertObject(any(RequestContext.class), eq(backwardMigrationConfig));
    verify(newRulesStore, times(1)).getAllConfigDataWithoutDefaults(any(RequestContext.class));
    verify(newRulesStore, times(1))
        .upsertObjects(
            any(RequestContext.class),
            argThat(list -> list.size() == 1 && verifyApiProtectionBackward(list.get(0))));

    // ==================== PHASE 4: FF DISABLED AGAIN - No work (nothing to rollback)
    // ====================
    resetStores();
    when(featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(any(RequestContext.class)))
        .thenReturn(false);
    mockMigrationStore(true, true, true, true, false); // migration NOT completed

    // Fourth call: Should return early (nothing to rollback)
    migrationManager.migrateForApiProtectionExclusionRulesIfApplicable(requestContext);

    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(0)).upsertObject(any(RequestContext.class), any());
    verify(newRulesStore, times(0)).getAllConfigDataWithoutDefaults(any(RequestContext.class));

    // ==================== PHASE 5: FF ENABLED AGAIN - Forward Migration Again ====================
    resetStores();
    when(featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(any(RequestContext.class)))
        .thenReturn(true);
    when(newRulesStore.getAllConfigDataWithoutDefaults(any()))
        .thenReturn(List.of(getSampleApiProtectionRule(false, "id4a")));

    DetectionExclusionMigrationConfig reMigrationConfig =
        mockMigrationStore(true, true, true, true, false).toBuilder()
            .setApiProtectionExclusionRulesMigrationCompleted(true)
            .build();

    // Fifth call: Forward migration should happen again
    migrationManager.migrateForApiProtectionExclusionRulesIfApplicable(requestContext);

    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(1)).upsertObject(any(RequestContext.class), eq(reMigrationConfig));
    verify(newRulesStore, times(1)).getAllConfigDataWithoutDefaults(any(RequestContext.class));
    verify(newRulesStore, times(1))
        .upsertObjects(
            any(RequestContext.class),
            argThat(list -> list.size() == 1 && verifyApiProtectionForward(list.get(0))));
  }

  @Test
  void testMigration_apiProtectionExclusionRules_contentTypeMultipleEvents() {
    when(featureCachingClient.isApiProtectConfigPoliciesRevampEnabled(any(RequestContext.class)))
        .thenReturn(true);
    when(newRulesStore.getAllConfigDataWithoutDefaults(any()))
        .thenReturn(
            List.of(
                sampleNewRule,
                getSampleApiProtectionContentTypeRule(false, "id5a"),
                getSampleApiProtectionContentTypeRule(true, "id5b")));
    DetectionExclusionMigrationConfig completedMigrationConfig =
        mockMigrationStore(true, true, true, true, false).toBuilder()
            .setApiProtectionExclusionRulesMigrationCompleted(true)
            .build();

    migrationManager.migrateForApiProtectionExclusionRulesIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(1))
        .upsertObject(any(RequestContext.class), eq(completedMigrationConfig));
    verify(newRulesStore, times(1)).getAllConfigDataWithoutDefaults(any(RequestContext.class));
    verify(newRulesStore, times(1)).upsertObjects(any(RequestContext.class), any());
    verify(newRulesStore, times(1))
        .upsertObjects(
            any(RequestContext.class),
            argThat(list -> list.size() == 1 && verifyContentTypeMultipleEvents(list.get(0))));

    resetStores();
    migrationManager.migrateForApiProtectionExclusionRulesIfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  private AnomalyExclusionRuleConfig getSampleOldRuleConfig() {
    return AnomalyExclusionRuleConfig.newBuilder()
        .setId("id1")
        .setConfigStatus(AnomalyConfigStatus.newBuilder().setInternal(true))
        .setRuleData(
            AnomalyExclusionRuleData.newBuilder()
                .setName("name1")
                .setDescription("desc1")
                .setEventExclusionInfo(
                    EventExclusionInfo.newBuilder()
                        .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
                        .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE)
                        .setEventTypeId("crs_941112"))
                .setAnomalyConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setServiceScope(
                            AnomalyServiceScope.newBuilder()
                                .setId("service")
                                .setEnvironmentScope(
                                    AnomalyEnvironmentScope.newBuilder().setEnvironmentId("env")))))
        .build();
  }

  private DetectionExclusionRule getSampleNewRule() {
    return DetectionExclusionRule.newBuilder()
        .setId("id1")
        .setRuleScope(
            DetectionExclusionRuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env")))
        .setRuleInfo(
            DetectionExclusionRuleInfo.newBuilder()
                .setName("name1")
                .setDescription("desc1")
                .setRuleStatus(
                    DetectionExclusionRuleStatus.newBuilder()
                        .setGenerateInternalEvents(true)
                        .setRuleCreationSource(RuleSource.RULE_SOURCE_OLD_API))
                .addConditions(
                    DetectionExclusionCondition.newBuilder()
                        .setEventCondition(
                            EventCondition.newBuilder()
                                .addSystemDefinedEvents(
                                    SystemDefinedEvent.newBuilder()
                                        .setEventFamily(
                                            SystemDefinedEventFamily
                                                .SYSTEM_DEFINED_EVENT_FAMILY_MODSEC)
                                        .setEventTypeId("crs_941"))))
                .addConditions(
                    DetectionExclusionCondition.newBuilder()
                        .setScopeCondition(
                            ScopeCondition.newBuilder()
                                .setEntityScope(
                                    EntityScope.newBuilder()
                                        .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                        .addEntityIds("service")))))
        .build();
  }

  private DetectionExclusionRule getSampleChangeLog2Rule(boolean updated, String id) {
    DetectionExclusionRuleInfo.Builder ruleInfoBuilder =
        DetectionExclusionRuleInfo.newBuilder()
            .setName("name3")
            .setDescription("desc3")
            .addConditions(
                DetectionExclusionCondition.newBuilder()
                    .setScopeCondition(
                        ScopeCondition.newBuilder()
                            .setEntityScope(
                                EntityScope.newBuilder()
                                    .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                    .addEntityIds("service"))));
    if (updated) {
      ruleInfoBuilder.addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_BLOCK);
    }
    return DetectionExclusionRule.newBuilder()
        .setId(id)
        .setRuleScope(
            DetectionExclusionRuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env")))
        .setRuleInfo(ruleInfoBuilder)
        .build();
  }

  private DetectionExclusionRule getSampleSstiRule(boolean updated, String id) {
    return DetectionExclusionRule.newBuilder()
        .setId(id)
        .setRuleScope(
            DetectionExclusionRuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env")))
        .setRuleInfo(
            DetectionExclusionRuleInfo.newBuilder()
                .setName("name2")
                .setDescription("desc2")
                .addConditions(
                    DetectionExclusionCondition.newBuilder()
                        .setEventCondition(
                            EventCondition.newBuilder()
                                .addSystemDefinedEvents(
                                    SystemDefinedEvent.newBuilder()
                                        .setEventFamily(
                                            SystemDefinedEventFamily
                                                .SYSTEM_DEFINED_EVENT_FAMILY_MODSEC)
                                        .setEventSubTypeId(
                                            updated ? "crs_9320310" : "crs_9210310"))))
                .addConditions(
                    DetectionExclusionCondition.newBuilder()
                        .setScopeCondition(
                            ScopeCondition.newBuilder()
                                .setEntityScope(
                                    EntityScope.newBuilder()
                                        .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                        .addEntityIds("service")))))
        .build();
  }

  private DetectionExclusionRule getSampleSourceRule(boolean markedAsHidden, String id) {
    return DetectionExclusionRule.newBuilder()
        .setId(id)
        .setRuleInfo(
            DetectionExclusionRuleInfo.newBuilder()
                .addConditions(
                    DetectionExclusionCondition.newBuilder()
                        .setSourceScopeCondition(
                            ScopeCondition.newBuilder()
                                .setEntityScope(
                                    EntityScope.newBuilder()
                                        .setEntityType(EntityType.ENTITY_TYPE_API)
                                        .addEntityIds("sourceApi"))))
                .setRuleStatus(DetectionExclusionRuleStatus.newBuilder().setHidden(markedAsHidden)))
        .build();
  }

  private DetectionExclusionRule getSampleApiProtectionRule(boolean updated, String id) {
    SystemDefinedEvent.Builder eventBuilder = SystemDefinedEvent.newBuilder();
    if (updated) {
      eventBuilder.setEventSubTypeId("authzh_obola");
    } else {
      eventBuilder.setEventTypeId("bola");
    }
    return DetectionExclusionRule.newBuilder()
        .setId(id)
        .setRuleScope(
            DetectionExclusionRuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env")))
        .setRuleInfo(
            DetectionExclusionRuleInfo.newBuilder()
                .setName("name4")
                .setDescription("desc4")
                .addConditions(
                    DetectionExclusionCondition.newBuilder()
                        .setEventCondition(
                            EventCondition.newBuilder().addSystemDefinedEvents(eventBuilder)))
                .addConditions(
                    DetectionExclusionCondition.newBuilder()
                        .setScopeCondition(
                            ScopeCondition.newBuilder()
                                .setEntityScope(
                                    EntityScope.newBuilder()
                                        .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                        .addEntityIds("service")))))
        .build();
  }

  private boolean verifyApiProtectionForward(DetectionExclusionRule rule) {
    assertEquals("id4a", rule.getId());
    List<SystemDefinedEvent> events =
        rule.getRuleInfo().getConditionsList().stream()
            .flatMap(
                condition -> condition.getEventCondition().getSystemDefinedEventsList().stream())
            .filter(
                event -> !event.getEventTypeId().isEmpty() || !event.getEventSubTypeId().isEmpty())
            .collect(Collectors.toList());

    // Verify we have only the new event (1 total) - old "bola" converted to "authzh_obola"
    assertEquals(1, events.size());

    // Verify the event is converted to new subTypeId
    assertTrue(
        events.stream()
            .allMatch(
                event ->
                    event.getEventTypeId().isEmpty()
                        && event.getEventSubTypeId().equals("authzh_obola")));
    return true;
  }

  private boolean verifyApiProtectionBackward(DetectionExclusionRule rule) {
    assertEquals("id4a", rule.getId());
    List<SystemDefinedEvent> events =
        rule.getRuleInfo().getConditionsList().stream()
            .flatMap(
                condition -> condition.getEventCondition().getSystemDefinedEventsList().stream())
            .filter(
                event -> !event.getEventTypeId().isEmpty() || !event.getEventSubTypeId().isEmpty())
            .collect(Collectors.toList());

    // Verify we have only the old event (1 total) - "authzh_obola" converted back to "bola"
    assertEquals(1, events.size());

    // Verify the event is converted back to old typeId
    assertTrue(
        events.stream()
            .allMatch(
                event ->
                    event.getEventSubTypeId().isEmpty() && event.getEventTypeId().equals("bola")));
    return true;
  }

  private DetectionExclusionRule getSampleApiProtectionContentTypeRule(boolean updated, String id) {
    DetectionExclusionRuleInfo.Builder ruleInfoBuilder =
        DetectionExclusionRuleInfo.newBuilder()
            .setName("name5")
            .setDescription("desc5")
            .addConditions(
                DetectionExclusionCondition.newBuilder()
                    .setScopeCondition(
                        ScopeCondition.newBuilder()
                            .setEntityScope(
                                EntityScope.newBuilder()
                                    .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                    .addEntityIds("service"))));

    if (updated) {
      ruleInfoBuilder.addConditions(
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(
                  EventCondition.newBuilder()
                      .addSystemDefinedEvents(
                          SystemDefinedEvent.newBuilder()
                              .setEventFamily(
                                  SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_API_DEF)
                              .setEventSubTypeId("contentAnomaly_reqctm"))
                      .addSystemDefinedEvents(
                          SystemDefinedEvent.newBuilder()
                              .setEventFamily(
                                  SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_API_DEF)
                              .setEventSubTypeId("schemaValidation_reqctve"))
                      .addSystemDefinedEvents(
                          SystemDefinedEvent.newBuilder()
                              .setEventFamily(
                                  SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_API_DEF)
                              .setEventSubTypeId("schemaValidation_resctve"))));
    } else {
      ruleInfoBuilder.addConditions(
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(
                  EventCondition.newBuilder()
                      .addSystemDefinedEvents(
                          SystemDefinedEvent.newBuilder()
                              .setEventFamily(
                                  SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_API_DEF)
                              .setEventTypeId("contentType"))));
    }

    return DetectionExclusionRule.newBuilder()
        .setId(id)
        .setRuleScope(
            DetectionExclusionRuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env")))
        .setRuleInfo(ruleInfoBuilder)
        .build();
  }

  private boolean verifyContentTypeMultipleEvents(DetectionExclusionRule rule) {
    assertEquals("id5a", rule.getId());
    List<SystemDefinedEvent> events =
        rule.getRuleInfo().getConditionsList().stream()
            .flatMap(
                condition -> condition.getEventCondition().getSystemDefinedEventsList().stream())
            .collect(Collectors.toList());

    // Verify we have exactly 3 events (old "contentType" converted to 3 new subTypeIds)
    assertEquals(3, events.size());

    // Verify all events have empty typeId (converted to subTypeId)
    assertTrue(events.stream().allMatch(event -> event.getEventTypeId().isEmpty()));

    // Verify the 3 new events with correct subTypeIds
    List<String> subTypeIds =
        events.stream().map(SystemDefinedEvent::getEventSubTypeId).collect(Collectors.toList());
    List<SystemDefinedEventFamily> families =
        events.stream().map(SystemDefinedEvent::getEventFamily).collect(Collectors.toList());

    assertTrue(subTypeIds.contains("contentAnomaly_reqctm"));
    assertTrue(subTypeIds.contains("schemaValidation_reqctve"));
    assertTrue(subTypeIds.contains("schemaValidation_resctve"));
    assertEquals(1, families.stream().distinct().count());
    assertEquals(SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_API_DEF, families.get(0));

    return true;
  }

  private boolean verifyContentTypeBackward(DetectionExclusionRule rule) {
    assertEquals("id5a", rule.getId());
    List<SystemDefinedEvent> events =
        rule.getRuleInfo().getConditionsList().stream()
            .flatMap(
                condition -> condition.getEventCondition().getSystemDefinedEventsList().stream())
            .collect(Collectors.toList());

    // Verify we have exactly 1 event (3 new subTypeIds converted back to old "contentType")
    assertEquals(1, events.size());

    // Verify the event is converted back to old typeId
    SystemDefinedEvent event = events.get(0);
    assertEquals("contentType", event.getEventTypeId());
    assertTrue(event.getEventSubTypeId().isEmpty());
    assertEquals(
        SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_API_DEF, event.getEventFamily());

    return true;
  }

  private SampleContextualConfigObject<AnomalyExclusionRuleConfig> getOldRuleContextualConfigObject(
      String context, Instant lastUpdatedTimestamp) {
    return new SampleContextualConfigObject<>(
        sampleOldRuleConfig.toBuilder().setId(context).build(),
        context,
        creationTimestamp,
        "system",
        lastUpdatedTimestamp,
        "system",
        lastUpdatedTimestamp);
  }

  private SampleContextualConfigObject<DetectionExclusionRule> getNewRuleContextualConfigObject(
      String context, Instant lastUpdatedTimestamp) {
    return new SampleContextualConfigObject<>(
        sampleNewRule.toBuilder().setId(context).build(),
        context,
        creationTimestamp,
        "system",
        lastUpdatedTimestamp,
        "system",
        lastUpdatedTimestamp);
  }

  private DetectionExclusionMigrationConfig mockMigrationStore(
      boolean migrationCompleted,
      boolean changeLog2MigrationCompleted,
      boolean changeLog3MigrationCompleted,
      boolean changeLog4MigrationCompleted) {
    return mockMigrationStore(
        migrationCompleted,
        changeLog2MigrationCompleted,
        changeLog3MigrationCompleted,
        changeLog4MigrationCompleted,
        false);
  }

  private DetectionExclusionMigrationConfig mockMigrationStore(
      boolean migrationCompleted,
      boolean changeLog2MigrationCompleted,
      boolean changeLog3MigrationCompleted,
      boolean changeLog4MigrationCompleted,
      boolean apiProtectionExclusionRulesMigrationCompleted) {
    return mockMigrationStore(
        migrationCompleted,
        changeLog2MigrationCompleted,
        changeLog3MigrationCompleted,
        changeLog4MigrationCompleted,
        apiProtectionExclusionRulesMigrationCompleted,
        false,
        false);
  }

  private DetectionExclusionMigrationConfig mockMigrationStore(
      boolean migrationCompleted,
      boolean changeLog2MigrationCompleted,
      boolean changeLog3MigrationCompleted,
      boolean changeLog4MigrationCompleted,
      boolean apiProtectionExclusionRulesMigrationCompleted,
      boolean ruleEvaluationPointsMigrationCompleted,
      boolean allowOnlyPlatformRemovalMigrationCompleted) {
    DetectionExclusionMigrationConfig migrationConfig =
        DetectionExclusionMigrationConfig.newBuilder()
            .setMigrationCompleted(migrationCompleted)
            .setChangeLog2MigrationCompleted(changeLog2MigrationCompleted)
            .setChangeLog3MigrationCompleted(changeLog3MigrationCompleted)
            .setChangeLog4MigrationCompleted(changeLog4MigrationCompleted)
            .setApiProtectionExclusionRulesMigrationCompleted(
                apiProtectionExclusionRulesMigrationCompleted)
            .setRuleEvaluationPointsMigrationCompleted(ruleEvaluationPointsMigrationCompleted)
            .setAllowOnlyPlatformRemovalMigrationCompleted(
                allowOnlyPlatformRemovalMigrationCompleted)
            .build();
    when(migrationStore.getData(any())).thenReturn(Optional.of(migrationConfig));
    return migrationConfig;
  }

  private void resetStores() {
    reset(newRulesStore);
    reset(oldRulesStore);
    reset(migrationStore);
  }

  private void verifyZeroInteractionWithRulesStore(boolean verifyMigrationStore) {
    if (verifyMigrationStore) {
      verify(migrationStore, times(0)).getData(any(RequestContext.class));
    }
    verify(oldRulesStore, times(0)).getAllObjects(any(RequestContext.class));
    verify(newRulesStore, times(0)).getAllObjects(any(RequestContext.class), any());
    verify(newRulesStore, times(0)).upsertObjects(any(RequestContext.class), any());
  }

  private static class SampleContextualConfigObject<T> implements ContextualConfigObject<T> {

    private final T data;
    private final String context;
    private final Instant creationTimestamp;
    private final String createdByEmail;
    private final Instant lastUserUpdateTimestamp;
    private final String lastUserUpdateEmail;
    private final Instant lastUpdatedTimestamp;
    private final String lastUpdateEmail;

    SampleContextualConfigObject(
        T data,
        String context,
        Instant creationTimestamp,
        String createdByEmail,
        Instant lastUserUpdateTimestamp,
        String lastUserUpdateEmail,
        Instant lastUpdatedTimestamp) {
      this.data = data;
      this.context = context;
      this.creationTimestamp = creationTimestamp;
      this.createdByEmail = createdByEmail;
      this.lastUserUpdateTimestamp = lastUserUpdateTimestamp;
      this.lastUserUpdateEmail = lastUserUpdateEmail;
      this.lastUpdatedTimestamp = lastUpdatedTimestamp;
      this.lastUpdateEmail = "system";
    }

    @Override
    public T getData() {
      return data;
    }

    @Override
    public Instant getCreationTimestamp() {
      return creationTimestamp;
    }

    @Override
    public String getCreatedByEmail() {
      return createdByEmail;
    }

    @Override
    public Instant getLastUserUpdateTimestamp() {
      return lastUserUpdateTimestamp;
    }

    @Override
    public String getLastUserUpdateEmail() {
      return lastUserUpdateEmail;
    }

    @Override
    public Instant getLastUpdatedTimestamp() {
      return lastUpdatedTimestamp;
    }

    @Override
    public String getLastUpdateEmail() {
      return lastUpdateEmail;
    }

    @Override
    public String getContext() {
      return context;
    }
  }

  @Test
  void testMigration1_ruleEvaluationPoints() {
    when(newRulesStore.getAllConfigData(any()))
        .thenReturn(
            List.of(
                sampleNewRule,
                getSampleRuleWithoutRuleEvaluationPoints(),
                getSampleRuleWithRuleEvaluationPoints()));
    DetectionExclusionMigrationConfig completedMigrationConfig =
        mockMigrationStore(true, true, true, true, false, false, false).toBuilder()
            .setRuleEvaluationPointsMigrationCompleted(true)
            .build();

    migrationManager.migrateForRuleEvaluationPointsIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(1))
        .upsertObject(any(RequestContext.class), eq(completedMigrationConfig));
    verify(newRulesStore, times(1)).getAllConfigData(any(RequestContext.class));
    verify(newRulesStore, times(1)).upsertObjects(any(RequestContext.class), any());
    verify(newRulesStore, times(1))
        .upsertObjects(
            any(RequestContext.class),
            argThat(
                list ->
                    list.size() == 2
                        && list.stream()
                            .noneMatch(
                                rule ->
                                    rule.getRuleInfo().getRuleEvaluationPointsList().isEmpty())));

    resetStores();
    migrationManager.migrateForRuleEvaluationPointsIfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  @Test
  void testMigrationCompleted2_ruleEvaluationPoints() {
    when(newRulesStore.getAllConfigData(any()))
        .thenReturn(
            List.of(
                sampleNewRule,
                getSampleRuleWithoutRuleEvaluationPoints(),
                getSampleRuleWithRuleEvaluationPoints()));
    mockMigrationStore(false, false, false, false, false, true, false);

    migrationManager.migrateForRuleEvaluationPointsIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(0)).upsertObject(any(RequestContext.class), any());
    verify(newRulesStore, times(0)).getAllConfigData(any(RequestContext.class));

    resetStores();
    migrationManager.migrateForRuleEvaluationPointsIfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  @Test
  void testMigration1_allowOnlyPlatformRemoval() {
    when(newRulesStore.getAllConfigData(any()))
        .thenReturn(
            List.of(
                sampleNewRule,
                getSampleAllowOnlyWithPlatform(),
                getSampleAllowOnlyWithPlatformAndEdge(),
                getSampleAllowOnlyWithoutPlatform("id7c"),
                getSampleBlockWithPlatform()));
    DetectionExclusionMigrationConfig completedMigrationConfig =
        mockMigrationStore(true, true, true, true, false, true, false).toBuilder()
            .setAllowOnlyPlatformRemovalMigrationCompleted(true)
            .build();

    migrationManager.migrateForAllowOnlyPlatformRemovalIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(1))
        .upsertObject(any(RequestContext.class), eq(completedMigrationConfig));
    verify(newRulesStore, times(1)).getAllConfigData(any(RequestContext.class));
    verify(newRulesStore, times(1)).upsertObjects(any(RequestContext.class), any());
    verify(newRulesStore, times(1))
        .upsertObjects(
            any(RequestContext.class),
            argThat(
                list -> {
                  if (list.size() != 2) {
                    return false;
                  }
                  long fallbackCount =
                      list.stream()
                          .filter(
                              rule -> {
                                List<RuleEvaluationPoint> reps =
                                    rule.getRuleInfo().getRuleEvaluationPointsList();
                                return reps.size() == 1
                                    && reps.contains(
                                        RuleEvaluationPoint
                                            .RULE_EVALUATION_POINT_INLINE_TRACING_AGENT);
                              })
                          .count();
                  long edgeOnlyCount =
                      list.stream()
                          .filter(
                              rule -> {
                                List<RuleEvaluationPoint> reps =
                                    rule.getRuleInfo().getRuleEvaluationPointsList();
                                return reps.contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                                    && !reps.contains(
                                        RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM);
                              })
                          .count();
                  return fallbackCount == 1 && edgeOnlyCount == 1;
                }));

    resetStores();
    migrationManager.migrateForAllowOnlyPlatformRemovalIfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  @Test
  void testMigrationCompleted2_allowOnlyPlatformRemoval() {
    when(newRulesStore.getAllConfigData(any()))
        .thenReturn(
            List.of(
                sampleNewRule,
                getSampleAllowOnlyWithPlatform(),
                getSampleAllowOnlyWithoutPlatform("id7b")));
    mockMigrationStore(false, false, false, false, false, false, true);

    migrationManager.migrateForAllowOnlyPlatformRemovalIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(any(RequestContext.class));
    verify(migrationStore, times(0)).upsertObject(any(RequestContext.class), any());
    verify(newRulesStore, times(0)).getAllConfigData(any(RequestContext.class));

    resetStores();
    migrationManager.migrateForAllowOnlyPlatformRemovalIfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  private DetectionExclusionRule getSampleRuleWithoutRuleEvaluationPoints() {
    return DetectionExclusionRule.newBuilder()
        .setId("rule-id")
        .setRuleScope(
            DetectionExclusionRuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env")))
        .setRuleInfo(
            DetectionExclusionRuleInfo.newBuilder()
                .setName("rule-name")
                .setDescription("rule-description")
                .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_ALERT)
                .addConditions(
                    DetectionExclusionCondition.newBuilder()
                        .setScopeCondition(
                            ScopeCondition.newBuilder()
                                .setEntityScope(
                                    EntityScope.newBuilder()
                                        .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                        .addEntityIds("service")))))
        .build();
  }

  private DetectionExclusionRule getSampleRuleWithRuleEvaluationPoints() {
    return DetectionExclusionRule.newBuilder()
        .setId("rule-id")
        .setRuleScope(
            DetectionExclusionRuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env")))
        .setRuleInfo(
            DetectionExclusionRuleInfo.newBuilder()
                .setName("rule-name")
                .setDescription("rule-description")
                .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_ALERT)
                .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .addConditions(
                    DetectionExclusionCondition.newBuilder()
                        .setScopeCondition(
                            ScopeCondition.newBuilder()
                                .setEntityScope(
                                    EntityScope.newBuilder()
                                        .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                        .addEntityIds("service")))))
        .build();
  }

  private DetectionExclusionRule getSampleAllowOnlyWithPlatform() {
    return DetectionExclusionRule.newBuilder()
        .setId("rule-id")
        .setRuleScope(
            DetectionExclusionRuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env")))
        .setRuleInfo(
            DetectionExclusionRuleInfo.newBuilder()
                .setName("rule-name")
                .setDescription("rule-description")
                .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_ALLOW)
                .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .addConditions(
                    DetectionExclusionCondition.newBuilder()
                        .setScopeCondition(
                            ScopeCondition.newBuilder()
                                .setEntityScope(
                                    EntityScope.newBuilder()
                                        .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                        .addEntityIds("service")))))
        .build();
  }

  private DetectionExclusionRule getSampleAllowOnlyWithPlatformAndEdge() {
    return DetectionExclusionRule.newBuilder()
        .setId("rule-id")
        .setRuleScope(
            DetectionExclusionRuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env")))
        .setRuleInfo(
            DetectionExclusionRuleInfo.newBuilder()
                .setName("rule-name")
                .setDescription("rule-description")
                .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_ALLOW)
                .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .addConditions(
                    DetectionExclusionCondition.newBuilder()
                        .setScopeCondition(
                            ScopeCondition.newBuilder()
                                .setEntityScope(
                                    EntityScope.newBuilder()
                                        .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                        .addEntityIds("service")))))
        .build();
  }

  private DetectionExclusionRule getSampleAllowOnlyWithoutPlatform(String id) {
    return DetectionExclusionRule.newBuilder()
        .setId(id)
        .setRuleScope(
            DetectionExclusionRuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env")))
        .setRuleInfo(
            DetectionExclusionRuleInfo.newBuilder()
                .setName("name7c")
                .setDescription("desc7c")
                .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_ALLOW)
                .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                .addConditions(
                    DetectionExclusionCondition.newBuilder()
                        .setScopeCondition(
                            ScopeCondition.newBuilder()
                                .setEntityScope(
                                    EntityScope.newBuilder()
                                        .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                        .addEntityIds("service")))))
        .build();
  }

  private DetectionExclusionRule getSampleBlockWithPlatform() {
    return DetectionExclusionRule.newBuilder()
        .setId("rule-id")
        .setRuleScope(
            DetectionExclusionRuleScope.newBuilder()
                .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env")))
        .setRuleInfo(
            DetectionExclusionRuleInfo.newBuilder()
                .setName("rule-name")
                .setDescription("rule-description")
                .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_BLOCK)
                .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                .addConditions(
                    DetectionExclusionCondition.newBuilder()
                        .setScopeCondition(
                            ScopeCondition.newBuilder()
                                .setEntityScope(
                                    EntityScope.newBuilder()
                                        .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                        .addEntityIds("service")))))
        .build();
  }
}
