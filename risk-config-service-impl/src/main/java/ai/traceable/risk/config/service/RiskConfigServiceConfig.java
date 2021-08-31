package ai.traceable.risk.config.service;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class RiskConfigServiceConfig {

  private static final String RISK_LEVEL_CONFIG_VALUES = "riskLevelConfigValues";
  private static final String RISK_FACTOR_GRID_CONFIG_VALUES = "riskFactorGridConfigValues";

  private final Config config;

  public RiskConfigServiceConfig(Config config) {
    this.config = config;
  }

  public Config getRiskLevelConfigValues() {
    return config.hasPath(RISK_LEVEL_CONFIG_VALUES)
        ? config.getConfig(RISK_LEVEL_CONFIG_VALUES)
        : ConfigFactory.empty();
  }

  public Config getRiskFactorGridConfigValues() {
    return config.hasPath(RISK_FACTOR_GRID_CONFIG_VALUES)
        ? config.getConfig(RISK_FACTOR_GRID_CONFIG_VALUES)
        : ConfigFactory.empty();
  }
}
