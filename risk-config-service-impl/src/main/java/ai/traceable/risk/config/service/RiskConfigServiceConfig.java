package ai.traceable.risk.config.service;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class RiskConfigServiceConfig {

  private static final String RISK_LEVEL_CONFIG_VALUES = "riskLevelConfigValues";
  private static final String RISK_FACTOR_GRID_CONFIG_VALUES = "riskFactorGridConfigValues";
  private static final String RISK_LIKELIHOOD_CONFIGS = "riskLikelihoodConfigs";
  private static final String RISK_IMPACT_CONFIGS = "riskImpactConfigs";

  private final Config config;

  public RiskConfigServiceConfig(Config config) {
    this.config = config;
  }

  public Config getRiskLevelConfigValues() {
    return getConfig(RISK_LEVEL_CONFIG_VALUES);
  }

  public Config getRiskFactorGridConfigValues() {
    return getConfig(RISK_FACTOR_GRID_CONFIG_VALUES);
  }

  public Config getRiskLikelihoodConfigs() {
    return getConfig(RISK_LIKELIHOOD_CONFIGS);
  }

  public Config getRiskImpactConfigs() {
    return getConfig(RISK_IMPACT_CONFIGS);
  }

  private Config getConfig(String path) {
    return config.hasPath(path) ? config.getConfig(path) : ConfigFactory.empty();
  }
}
