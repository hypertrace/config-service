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
            new DetectionExclusionConfigServiceConfig(ConfigFactory.empty()));
  }

  @Test
  void testMigration_oldRules() {
    when(featureCachingClient.isDetectionExclusionV2EnabledForTenant(requestContext))
        .thenReturn(true);
    when(newRulesStore.getAllObjects(any(), any())).thenReturn(newRules);
    when(oldRulesStore.getAllObjects(any())).thenReturn(oldRules);
    DetectionExclusionMigrationConfig completedMigrationConfig =
        mockMigrationStore(false, false, false, false).toBuilder()
            .setMigrationCompleted(true)
            .build();

    migrationManager.migrateFromOldStoreIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(requestContext);
    verify(migrationStore, times(1)).upsertObject(requestContext, completedMigrationConfig);
    verify(oldRulesStore, times(1)).getAllObjects(requestContext);
    verify(newRulesStore, times(1)).getAllObjects(requestContext, OLD_RULES_FILTER);
    verify(newRulesStore, times(2)).upsertObjects(eq(requestContext), any());
    verify(newRulesStore, times(1))
        .upsertObjects(
            eq(requestContext),
            argThat(list -> list.size() == 1 && list.get(0).getId().equals("id0")));
    verify(newRulesStore, times(1))
        .upsertObjects(
            eq(requestContext),
            argThat(list -> list.size() == 1 && list.get(0).getId().equals("id2")));

    resetStores();
    migrationManager.migrateFromOldStoreIfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  @Test
  void testMigration_noUpdate_noOldRules() {
    when(featureCachingClient.isDetectionExclusionV2EnabledForTenant(requestContext))
        .thenReturn(true);
    when(newRulesStore.getAllObjects(any(), any())).thenReturn(Collections.emptyList());
    when(oldRulesStore.getAllObjects(any())).thenReturn(Collections.emptyList());
    DetectionExclusionMigrationConfig completedMigrationConfig =
        mockMigrationStore(false, false, false, false).toBuilder()
            .setMigrationCompleted(true)
            .build();

    migrationManager.migrateFromOldStoreIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(requestContext);
    verify(migrationStore, times(1)).upsertObject(requestContext, completedMigrationConfig);
    verify(oldRulesStore, times(1)).getAllObjects(requestContext);
    verify(newRulesStore, times(1)).getAllObjects(requestContext, OLD_RULES_FILTER);
    verify(newRulesStore, times(0)).upsertObjects(eq(requestContext), any());

    resetStores();
    migrationManager.migrateFromOldStoreIfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  @Test
  void testMigration_noUpdate_migrationCompleted() {
    when(featureCachingClient.isDetectionExclusionV2EnabledForTenant(requestContext))
        .thenReturn(true);
    when(newRulesStore.getAllObjects(any(), any())).thenReturn(newRules);
    when(oldRulesStore.getAllObjects(any())).thenReturn(oldRules);
    mockMigrationStore(true, false, false, false);

    migrationManager.migrateFromOldStoreIfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(requestContext);
    verify(migrationStore, times(0)).upsertObject(eq(requestContext), any());
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
    verify(migrationStore, times(1)).getData(requestContext);
    verify(migrationStore, times(1)).upsertObject(requestContext, completedMigrationConfig);
    verify(newRulesStore, times(1)).getAllConfigData(requestContext);
    verify(newRulesStore, times(1)).upsertObjects(eq(requestContext), any());
    verify(newRulesStore, times(1))
        .upsertObjects(
            eq(requestContext),
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
    verify(migrationStore, times(1)).getData(requestContext);
    verify(migrationStore, times(0)).upsertObject(eq(requestContext), any());
    verify(newRulesStore, times(0)).getAllConfigData(requestContext);

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
    verify(migrationStore, times(1)).getData(requestContext);
    verify(migrationStore, times(1)).upsertObject(requestContext, completedMigrationConfig);
    verify(newRulesStore, times(1)).getAllConfigData(requestContext);
    verify(newRulesStore, times(1)).upsertObjects(eq(requestContext), any());
    verify(newRulesStore, times(1))
        .upsertObjects(
            eq(requestContext),
            argThat(list -> list.size() == 1 && verifySsti("id3a", list.get(0))));

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
    verify(migrationStore, times(1)).getData(requestContext);
    verify(migrationStore, times(1)).upsertObject(requestContext, completedMigrationConfig);
    verify(newRulesStore, times(1)).getAllConfigData(requestContext);
    verify(newRulesStore, times(1)).upsertObjects(eq(requestContext), any());
    verify(newRulesStore, times(1))
        .upsertObjects(
            eq(requestContext),
            argThat(list -> list.size() == 1 && verifyHidden("id3a", list.get(0))));

    resetStores();
    migrationManager.migrateFromChangeLog4IfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  private boolean verifyHidden(String id, DetectionExclusionRule rule) {
    assertEquals(id, rule.getId());
    assertTrue(rule.getRuleInfo().getRuleStatus().getHidden());
    return true;
  }

  private boolean verifySsti(String id, DetectionExclusionRule rule) {
    assertEquals(id, rule.getId());
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
    verify(migrationStore, times(1)).getData(requestContext);
    verify(migrationStore, times(0)).upsertObject(eq(requestContext), any());
    verify(newRulesStore, times(0)).getAllConfigData(requestContext);

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
    verify(migrationStore, times(1)).getData(requestContext);
    verify(migrationStore, times(0)).upsertObject(eq(requestContext), any());
    verify(newRulesStore, times(0)).getAllConfigData(requestContext);

    resetStores();
    migrationManager.migrateFromChangeLog4IfApplicable(requestContext);
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

  private SampleContextualConfigObject<AnomalyExclusionRuleConfig> getOldRuleContextualConfigObject(
      String context, Instant lastUpdatedTimestamp) {
    return new SampleContextualConfigObject<>(
        sampleOldRuleConfig.toBuilder().setId(context).build(),
        context,
        creationTimestamp,
        lastUpdatedTimestamp);
  }

  private SampleContextualConfigObject<DetectionExclusionRule> getNewRuleContextualConfigObject(
      String context, Instant lastUpdatedTimestamp) {
    return new SampleContextualConfigObject<>(
        sampleNewRule.toBuilder().setId(context).build(),
        context,
        creationTimestamp,
        lastUpdatedTimestamp);
  }

  private DetectionExclusionMigrationConfig mockMigrationStore(
      boolean migrationCompleted,
      boolean changeLog2MigrationCompleted,
      boolean changeLog3MigrationCompleted,
      boolean changeLog4MigrationCompleted) {
    DetectionExclusionMigrationConfig migrationConfig =
        DetectionExclusionMigrationConfig.newBuilder()
            .setMigrationCompleted(migrationCompleted)
            .setChangeLog2MigrationCompleted(changeLog2MigrationCompleted)
            .setChangeLog3MigrationCompleted(changeLog3MigrationCompleted)
            .setChangeLog4MigrationCompleted(changeLog4MigrationCompleted)
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
      verify(migrationStore, times(0)).getData(requestContext);
    }
    verify(oldRulesStore, times(0)).getAllObjects(requestContext);
    verify(newRulesStore, times(0)).getAllObjects(eq(requestContext), any());
    verify(newRulesStore, times(0)).upsertObjects(eq(requestContext), any());
  }

  private static class SampleContextualConfigObject<T> implements ContextualConfigObject<T> {

    private final T data;
    private final String context;
    private final Instant creationTimestamp;
    private final Instant lastUpdatedTimestamp;

    SampleContextualConfigObject(
        T data, String context, Instant creationTimestamp, Instant lastUpdatedTimestamp) {
      this.data = data;
      this.context = context;
      this.creationTimestamp = creationTimestamp;
      this.lastUpdatedTimestamp = lastUpdatedTimestamp;
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
    public Instant getLastUpdatedTimestamp() {
      return lastUpdatedTimestamp;
    }

    @Override
    public String getContext() {
      return context;
    }
  }
}
