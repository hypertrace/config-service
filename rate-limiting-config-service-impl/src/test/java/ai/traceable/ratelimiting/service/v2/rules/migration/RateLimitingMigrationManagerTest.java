package ai.traceable.ratelimiting.service.v2.rules.migration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.IpAddressCondition;
import ai.traceable.ratelimiting.config.service.v2.LeafCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingMigrationConfig;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.ResourceAccessThresholdConfig;
import ai.traceable.ratelimiting.config.service.v2.RuleStatus;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceConfig;
import ai.traceable.ratelimiting.service.v2.rules.RateLimitingRulesStore;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RateLimitingMigrationManagerTest {

  private final RequestContext requestContext = RequestContext.forTenantId("tenantId");
  private RateLimitingRulesStore rulesStore;
  private RateLimitingMigrationStore migrationStore;
  private RateLimitingMigrationManager migrationManager;

  @BeforeEach
  void setUp() {
    rulesStore = mock(RateLimitingRulesStore.class);
    migrationStore = mock(RateLimitingMigrationStore.class);
    RateLimitingConfigServiceConfig config = mock(RateLimitingConfigServiceConfig.class);
    migrationManager = new RateLimitingMigrationManagerImpl(migrationStore, rulesStore, config);
  }

  @Test
  void testMigration_changeLog4() {
    when(rulesStore.getAllConfigData(any()))
        .thenReturn(List.of(getRule("id1", true, false), getRule("id2", false, false)));
    RateLimitingMigrationConfig completedMigrationConfig =
        mockMigrationStore(false).toBuilder().setChangeLog1MigrationCompleted(true).build();

    migrationManager.migrateFromChangeLog1IfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(requestContext);
    verify(migrationStore, times(1)).upsertObject(requestContext, completedMigrationConfig);
    verify(rulesStore, times(1)).getAllConfigData(requestContext);
    verify(rulesStore, times(1)).upsertObjects(eq(requestContext), any());
    verify(rulesStore, times(1))
        .upsertObjects(
            eq(requestContext),
            argThat(list -> list.size() == 1 && list.get(0).equals(getRule("id1", true, true))));

    resetStores();
    migrationManager.migrateFromChangeLog1IfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  @Test
  void testMigrationCompleted_changeLog4() {
    when(rulesStore.getAllConfigData(any()))
        .thenReturn(List.of(getRule("id1", true, false), getRule("id2", false, false)));
    mockMigrationStore(true);

    migrationManager.migrateFromChangeLog1IfApplicable(requestContext);
    verify(migrationStore, times(1)).getData(requestContext);
    verify(migrationStore, times(0)).upsertObject(eq(requestContext), any());
    verifyZeroInteractionWithRulesStore(false);

    resetStores();
    migrationManager.migrateFromChangeLog1IfApplicable(requestContext);
    verifyZeroInteractionWithRulesStore(true);
  }

  private RateLimitingRule getRule(String id, boolean internal, boolean hidden) {
    return RateLimitingRule.newBuilder()
        .setId(id)
        .setData(
            RateLimitingRuleData.newBuilder()
                .setRuleStatus(RuleStatus.newBuilder().setInternal(internal).setHidden(hidden))
                .setCondition(
                    Condition.newBuilder()
                        .setLeafCondition(
                            LeafCondition.newBuilder()
                                .setIpAddressCondition(
                                    IpAddressCondition.newBuilder().addIpAddresses("1.2.3.4"))))
                .setCategory(Category.CATEGORY_RATE_LIMITING)
                .addThresholdActionConfigs(
                    ThresholdActionConfig.newBuilder()
                        .addActions(Action.newBuilder().setBlock(Action.Block.getDefaultInstance()))
                        .addResourceAccessThresholdConfigs(
                            ResourceAccessThresholdConfig.newBuilder()
                                .setRollingWindowThresholdConfig(
                                    ResourceAccessThresholdConfig.RollingWindowThresholdConfig
                                        .newBuilder()
                                        .setCountAllowed(10)
                                        .setDurationIso("PT1M")))))
        .build();
  }

  private RateLimitingMigrationConfig mockMigrationStore(boolean changeLog1MigrationCompleted) {
    RateLimitingMigrationConfig migrationConfig =
        RateLimitingMigrationConfig.newBuilder()
            .setChangeLog1MigrationCompleted(changeLog1MigrationCompleted)
            .build();
    when(migrationStore.getData(any())).thenReturn(Optional.of(migrationConfig));
    return migrationConfig;
  }

  private void resetStores() {
    reset(rulesStore);
    reset(migrationStore);
  }

  private void verifyZeroInteractionWithRulesStore(boolean verifyMigrationStore) {
    if (verifyMigrationStore) {
      verify(migrationStore, times(0)).getData(requestContext);
    }
    verify(rulesStore, times(0)).getAllObjects(requestContext);
    verify(rulesStore, times(0)).upsertObjects(eq(requestContext), any());
  }
}
