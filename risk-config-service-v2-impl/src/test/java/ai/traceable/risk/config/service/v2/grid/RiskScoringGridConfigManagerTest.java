package ai.traceable.risk.config.service.v2.grid;

import static ai.traceable.risk.config.service.v2.grid.MockGridConfigsData.getDefaultGridConfig;
import static ai.traceable.risk.config.service.v2.scope.MockScopeData.getEnvironmentBasedScope;
import static ai.traceable.risk.config.service.v2.scope.MockScopeData.getGlobalRiskConfigScope;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.risk.config.service.v2.RiskConfigIdGenerator;
import ai.traceable.risk.config.service.v2.RiskConfigScope;
import ai.traceable.risk.config.service.v2.RiskScoreCategory;
import ai.traceable.risk.config.service.v2.RiskScoringGridCell;
import ai.traceable.risk.config.service.v2.RiskScoringGridConfig;
import ai.traceable.risk.config.service.v2.RiskScoringGridConfigValues;
import ai.traceable.risk.config.service.v2.grid.builder.RiskScoringGridConfigBuilder;
import ai.traceable.risk.config.service.v2.grid.comparator.RiskScoringGridConfigComparator;
import ai.traceable.risk.config.service.v2.grid.comparator.RiskScoringGridConfigComparatorImpl;
import ai.traceable.risk.config.service.v2.grid.manager.RiskScoringGridConfigManager;
import ai.traceable.risk.config.service.v2.grid.manager.RiskScoringGridConfigManagerImpl;
import java.util.Collections;
import java.util.List;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RiskScoringGridConfigManagerTest {
  private RiskScoringGridConfigValues defaultRiskScoringGridConfigValues;
  private RiskScoringGridConfigManager riskScoringGridConfigManager;
  private RiskConfigScope environmentRiskConfigScope;
  private RiskConfigScope globalRiskConfigScope;

  private static final RiskScoringGridConfigBuilder configBuilder =
      new RiskScoringGridConfigBuilder();
  private final RiskScoringGridConfigComparator comparator =
      new RiskScoringGridConfigComparatorImpl();

  @BeforeEach
  public void setup() {
    defaultRiskScoringGridConfigValues = getDefaultGridConfig();
    environmentRiskConfigScope = getEnvironmentBasedScope();
    globalRiskConfigScope = getGlobalRiskConfigScope();
    RiskConfigIdGenerator configIdGenerator = new RiskConfigIdGenerator(new UuidGenerator());

    IdentifiedObjectStore<RiskScoringGridConfigValues> configStore =
        new MockGridConfigsData.MockRiskScoringGridConfigStore(
            null, null, null, mock(ConfigChangeEventGenerator.class), configIdGenerator);
    riskScoringGridConfigManager =
        new RiskScoringGridConfigManagerImpl(
            configStore,
            configBuilder,
            comparator,
            defaultRiskScoringGridConfigValues,
            configIdGenerator);
  }

  @Test
  public void testGetUpdateDeleteRiskScoringGridConfig() {
    RequestContext requestContext = RequestContext.forTenantId("tenant");

    RiskScoringGridConfig defaultRiskScoringGridConfig =
        riskScoringGridConfigManager.getRiskScoringGridConfig(
            requestContext, environmentRiskConfigScope);

    assertTrue(defaultRiskScoringGridConfig.getIsDefault());
    assertEquals(
        buildScopedConfigValues(defaultRiskScoringGridConfigValues, environmentRiskConfigScope),
        defaultRiskScoringGridConfig.getRiskScoringGridConfigValues());
    assertEquals(
        defaultRiskScoringGridConfig,
        riskScoringGridConfigManager.resetRiskScoringGridConfig(
            requestContext, environmentRiskConfigScope));

    assertEquals(
        defaultRiskScoringGridConfig,
        riskScoringGridConfigManager.updateRiskScoringGridConfig(
            requestContext,
            environmentRiskConfigScope,
            defaultRiskScoringGridConfigValues.getRiskScoringGridCellsList()));

    List<RiskScoringGridCell> updateGridCells =
        Collections.singletonList(
            RiskScoringGridCell.newBuilder()
                .setLikelihoodScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                .setImpactScoreCategory(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                .setScore(3)
                .build());

    assertEquals(
        4,
        riskScoringGridConfigManager
            .updateRiskScoringGridConfig(
                requestContext, environmentRiskConfigScope, updateGridCells)
            .getRiskScoringGridConfigValues()
            .getRiskScoringGridCellsCount());

    RiskScoringGridConfig riskScoringGridConfig =
        riskScoringGridConfigManager.getRiskScoringGridConfig(
            requestContext, environmentRiskConfigScope);
    assertFalse(riskScoringGridConfig.getIsDefault());
    assertEquals(
        4, riskScoringGridConfig.getRiskScoringGridConfigValues().getRiskScoringGridCellsCount());
    riskScoringGridConfig
        .getRiskScoringGridConfigValues()
        .getRiskScoringGridCellsList()
        .forEach(
            cell -> {
              if (cell.getLikelihoodScoreCategory()
                      .equals(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)
                  && cell.getImpactScoreCategory()
                      .equals(RiskScoreCategory.RISK_SCORE_CATEGORY_LOW)) {
                assertEquals(3, cell.getScore());
              }
            });
    assertEquals(
        defaultRiskScoringGridConfig,
        riskScoringGridConfigManager.resetRiskScoringGridConfig(
            requestContext, environmentRiskConfigScope));
    assertEquals(
        defaultRiskScoringGridConfig,
        riskScoringGridConfigManager.getRiskScoringGridConfig(
            requestContext, environmentRiskConfigScope));

    RiskScoringGridConfig riskScoringGridConfigGlobalScoped =
        riskScoringGridConfigManager.getRiskScoringGridConfig(
            requestContext, globalRiskConfigScope);

    assertEquals(
        globalRiskConfigScope,
        riskScoringGridConfigGlobalScoped.getRiskScoringGridConfigValues().getRiskConfigScope());

    RiskScoringGridConfig previousUpdateConfig =
        riskScoringGridConfigManager.updateRiskScoringGridConfig(
            requestContext, environmentRiskConfigScope, updateGridCells);
    RiskScoringGridConfig currentUpdatedConfig =
        riskScoringGridConfigManager.updateRiskScoringGridConfig(
            requestContext, environmentRiskConfigScope, Collections.emptyList());
    assertEquals(previousUpdateConfig, currentUpdatedConfig);
  }

  private RiskScoringGridConfigValues buildScopedConfigValues(
      RiskScoringGridConfigValues riskScoringGridConfigValues, RiskConfigScope riskConfigScope) {
    return RiskScoringGridConfigValues.newBuilder()
        .setRiskConfigScope(riskConfigScope)
        .addAllRiskScoringGridCells(riskScoringGridConfigValues.getRiskScoringGridCellsList())
        .build();
  }
}
