package ai.traceable.threatmanagement.config.service;

import com.typesafe.config.Config;

public class ThreatManagementConfigServiceConfig {
  private static final String DEFAULT_THREAT_UPPER_BOUND_MEDIUM_SCORE_KEY =
      "threat.management.config.service.upper.bound.score.medium";
  private static final String DEFAULT_THREAT_UPPER_BOUND_HIGH_SCORE_KEY =
      "threat.management.config.service.upper.bound.score.high";

  private final Config config;

  ThreatManagementConfigServiceConfig(Config config) {
    this.config = config;
  }

  public int getDefaultThreatUpperBoundMediumScore() {
    return config.getInt(DEFAULT_THREAT_UPPER_BOUND_MEDIUM_SCORE_KEY);
  }

  public int getDefaultThreatUpperBoundHighScore() {
    return config.getInt(DEFAULT_THREAT_UPPER_BOUND_HIGH_SCORE_KEY);
  }
}
