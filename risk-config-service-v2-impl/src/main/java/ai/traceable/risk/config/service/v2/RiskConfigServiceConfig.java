package ai.traceable.risk.config.service.v2;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class RiskConfigServiceConfig {

  private static final String RISK_SCORING_GRID_CONFIG_VALUES = "riskScoringGridConfigValues";
  private static final String RISK_SCORING_LEVEL_CONFIG_VALUES = "riskScoringLevelConfigValues";
  private static final String RISK_CONTRIBUTOR_CONFIG = "riskContributorConfigs";

  private final Config config;

  public RiskConfigServiceConfig(Config config) {
    this.config = config;
  }

  public Config getRiskScoringLevelConfigValues() {
    return getConfig(RISK_SCORING_LEVEL_CONFIG_VALUES);
  }

  public Config getRiskScoringGridConfigValues() {
    return getConfig(RISK_SCORING_GRID_CONFIG_VALUES);
  }

  public Config getRiskContributorConfigs() {
    return getConfig(RISK_CONTRIBUTOR_CONFIG);
  }

  private Config getConfig(String path) {
    return config.hasPath(path) ? config.getConfig(path) : ConfigFactory.empty();
  }
}
