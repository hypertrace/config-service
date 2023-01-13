package ai.traceable.risk.config.service.v2.grid.builder;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskScoreCategory;
import ai.traceable.risk.config.service.v2.RiskScoringGridCell;
import ai.traceable.risk.config.service.v2.RiskScoringGridConfigValues;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public class RiskScoringGridConfigBuilder extends RiskConfigBuilder<RiskScoringGridConfigValues> {

  @Override
  public RiskScoringGridConfigValues.Builder getNewBuilder() {
    return RiskScoringGridConfigValues.newBuilder();
  }

  @Override
  public RiskScoringGridConfigValues mergeConfigs(
      RiskScoringGridConfigValues highPriorityConfig,
      RiskScoringGridConfigValues lowPriorityConfig) {
    Table<RiskScoreCategory, RiskScoreCategory, RiskScoringGridCell> configsTable =
        HashBasedTable.create();
    lowPriorityConfig
        .getRiskScoringGridCellsList()
        .forEach(
            cell ->
                configsTable.put(
                    cell.getLikelihoodScoreCategory(), cell.getImpactScoreCategory(), cell));
    highPriorityConfig
        .getRiskScoringGridCellsList()
        .forEach(
            cell ->
                configsTable.put(
                    cell.getLikelihoodScoreCategory(), cell.getImpactScoreCategory(), cell));

    RiskScoringGridConfigValues.Builder riskScoringGridConfigBuilder =
        RiskScoringGridConfigValues.newBuilder().addAllRiskScoringGridCells(configsTable.values());
    if (lowPriorityConfig.hasRiskConfigScope()) {
      riskScoringGridConfigBuilder.setRiskConfigScope(lowPriorityConfig.getRiskConfigScope());
    }
    return riskScoringGridConfigBuilder.build();
  }
}
