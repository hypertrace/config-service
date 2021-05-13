package ai.traceable.threatmanagement.config.service;

import com.typesafe.config.Config;

public class ThreatManagementConfigServiceConfig {
  private static final String DEFAULT_THREAT_UPPER_BOUND_MEDIUM_SCORE_KEY =
      "threat.management.config.service.upper.bound.score.medium";
  private static final String DEFAULT_THREAT_UPPER_BOUND_HIGH_SCORE_KEY =
      "threat.management.config.service.upper.bound.score.high";
  private static final String DEFAULT_SECURITY_EVENT_CONTRIBUTION_ANOMALY_SCORE_KEY =
      "threat.management.config.service.security.event.contribution.score.anomaly";
  private static final String DEFAULT_SECURITY_EVENT_CONTRIBUTION_MEDIUM_SCORE_KEY =
      "threat.management.config.service.security.event.contribution.score.medium";
  private static final String DEFAULT_SECURITY_EVENT_CONTRIBUTION_HIGH_SCORE_KEY =
      "threat.management.config.service.security.event.contribution.score.high";

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

  public int getDefaultSecurityEventContributionAnomalyScore() {
    return config.getInt(DEFAULT_SECURITY_EVENT_CONTRIBUTION_ANOMALY_SCORE_KEY);
  }

  public int getDefaultSecurityEventContributionMediumScore() {
    return config.getInt(DEFAULT_SECURITY_EVENT_CONTRIBUTION_MEDIUM_SCORE_KEY);
  }

  public int getDefaultSecurityEventContributionHighScore() {
    return config.getInt(DEFAULT_SECURITY_EVENT_CONTRIBUTION_HIGH_SCORE_KEY);
  }
}
