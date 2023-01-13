package ai.traceable.risk.config.service.v2.grid.comparator;

import ai.traceable.risk.config.service.v2.RiskScoringGridConfigValues;

public interface RiskScoringGridConfigComparator {

  boolean isGridValuesEqual(
      RiskScoringGridConfigValues specificConfigValues,
      RiskScoringGridConfigValues defaultConfigValues);
}
