package ai.traceable.iprange.config.service.rules.migration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.iprange.config.service.IpRangeConfigServiceConfig;
import ai.traceable.iprange.config.service.rules.IpRangeRulesStore;
import ai.traceable.iprange.config.service.v1.IpRangeMigrationConfig;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IpRangeRulesMigrationManagerTest {
  private final RequestContext requestContext = RequestContext.forTenantId("tenantId");
  private IpRangeRulesStore rulesStore;
  private IpRangeRulesMigrationStore migrationStore;
  private IpRangeRulesMigrationManager migrationManager;

  @BeforeEach
  void setUp() {
    rulesStore = mock(IpRangeRulesStore.class);
    migrationStore = mock(IpRangeRulesMigrationStore.class);
    IpRangeConfigServiceConfig config = mock(IpRangeConfigServiceConfig.class);
    migrationManager = new IpRangeRulesMigrationManagerImpl(migrationStore, rulesStore, config);
  }

  @Test
  void testMigration_changeLog1() {
    when(rulesStore.getAllConfigData(any()))
        .thenReturn(List.of(getRule("id1", true, false), getRule("id2", false, false)));
    IpRangeMigrationConfig completedMigrationConfig =
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
  void testMigrationCompleted_changeLog1() {
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

  private IpRangeRule getRule(String id, boolean internal, boolean hidden) {
    return IpRangeRule.newBuilder().setId(id).setHidden(hidden).setInternal(internal).build();
  }

  private IpRangeMigrationConfig mockMigrationStore(boolean changeLog1MigrationCompleted) {
    IpRangeMigrationConfig migrationConfig =
        IpRangeMigrationConfig.newBuilder()
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
