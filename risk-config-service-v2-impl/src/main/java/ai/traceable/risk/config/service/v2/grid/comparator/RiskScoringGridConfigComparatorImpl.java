package ai.traceable.risk.config.service.v2.grid.comparator;

import ai.traceable.risk.config.service.v2.RiskScoringGridCell;
import ai.traceable.risk.config.service.v2.RiskScoringGridConfigValues;
import java.util.HashSet;
import java.util.Set;

public class RiskScoringGridConfigComparatorImpl implements RiskScoringGridConfigComparator {

  @Override
  public boolean isGridValuesEqual(
      RiskScoringGridConfigValues specificConfigValues,
      RiskScoringGridConfigValues defaultConfigValues) {
    Set<RiskScoringGridCell> configCellsSet =
        new HashSet<>(defaultConfigValues.getRiskScoringGridCellsList());
    for (RiskScoringGridCell cell : specificConfigValues.getRiskScoringGridCellsList()) {
      if (!configCellsSet.contains(cell)) {
        return false;
      }
    }
    return true;
  }
}
