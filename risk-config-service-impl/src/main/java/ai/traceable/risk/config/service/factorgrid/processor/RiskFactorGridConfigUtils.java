package ai.traceable.risk.config.service.factorgrid.processor;

import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskFactorGridCell;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfigValues;
import ai.traceable.risk.config.service.v1.RiskScoreCategory;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import io.grpc.Status;
import java.util.HashSet;
import java.util.Set;

public class RiskFactorGridConfigUtils extends RiskConfigUtils<RiskFactorGridConfigValues> {

  @Override
  public RiskFactorGridConfigValues.Builder getNewBuilder() {
    return RiskFactorGridConfigValues.newBuilder();
  }

  @Override
  public RiskFactorGridConfigValues mergeConfigs(
      RiskFactorGridConfigValues highPriorityConfig, RiskFactorGridConfigValues lowPriorityConfig) {

    Table<RiskScoreCategory, RiskScoreCategory, RiskFactorGridCell> configsTable =
        HashBasedTable.create();
    lowPriorityConfig
        .getRiskFactorGridCellsList()
        .forEach(
            cell ->
                configsTable.put(
                    cell.getLikelihoodScoreCategory(), cell.getImpactScoreCategory(), cell));
    highPriorityConfig
        .getRiskFactorGridCellsList()
        .forEach(
            cell ->
                configsTable.put(
                    cell.getLikelihoodScoreCategory(), cell.getImpactScoreCategory(), cell));
    return RiskFactorGridConfigValues.newBuilder()
        .addAllRiskFactorGridCells(configsTable.values())
        .build();
  }

  @Override
  public boolean isConfigDefault(
      RiskFactorGridConfigValues specificConfig, RiskFactorGridConfigValues defaultConfig) {
    Set<RiskFactorGridCell> configCellsSet =
        new HashSet<>(defaultConfig.getRiskFactorGridCellsList());
    for (RiskFactorGridCell cell : specificConfig.getRiskFactorGridCellsList()) {
      if (!configCellsSet.contains(cell)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Status validateConfig(RiskFactorGridConfigValues config) {
    for (RiskFactorGridCell cell : config.getRiskFactorGridCellsList()) {
      if (!isValidScore(cell.getScore())) {
        return Status.OUT_OF_RANGE.withDescription("Score should be between 0 and 10");
      }
      if (!isValidScoreCategory(cell.getLikelihoodScoreCategory())
          || !isValidScoreCategory(cell.getImpactScoreCategory())) {
        return Status.INVALID_ARGUMENT.withDescription("Invalid Score Category");
      }
    }
    return Status.OK;
  }
}
