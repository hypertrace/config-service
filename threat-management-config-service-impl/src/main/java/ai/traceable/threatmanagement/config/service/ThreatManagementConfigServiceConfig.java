package ai.traceable.threatmanagement.config.service;

import com.typesafe.config.Config;

public class ThreatManagementConfigServiceConfig {
  private static final String DEFAULT_THREAT_UPPER_BOUND_LOW_SCORE_KEY =
      "threat.management.config.service.upper.bound.score.low";
  private static final String DEFAULT_THREAT_UPPER_BOUND_MEDIUM_SCORE_KEY =
      "threat.management.config.service.upper.bound.score.medium";
  private static final String DEFAULT_THREAT_UPPER_BOUND_HIGH_SCORE_KEY =
      "threat.management.config.service.upper.bound.score.high";
  private static final String DEFAULT_SECURITY_EVENT_CONTRIBUTION_LOW_SCORE_KEY =
      "threat.management.config.service.security.event.contribution.score.low";
  private static final String DEFAULT_SECURITY_EVENT_CONTRIBUTION_MEDIUM_SCORE_KEY =
      "threat.management.config.service.security.event.contribution.score.medium";
  private static final String DEFAULT_SECURITY_EVENT_CONTRIBUTION_HIGH_SCORE_KEY =
      "threat.management.config.service.security.event.contribution.score.high";
  private static final String DEFAULT_SECURITY_EVENT_CONTRIBUTION_CRITICAL_SCORE_KEY =
      "threat.management.config.service.security.event.contribution.score.critical";
  private static final String DEFAULT_ANOMALY_CONTRIBUTION_SCORE_KEY =
      "threat.management.config.service.anomaly.contribution.score";
  private static final String DEFAULT_CRITICAL_IP_REPUTATION_THREAT_SCORE_INCREMENT =
      "threat.management.config.service.ip.reputation.threat.score.increment.critical";
  private static final String DEFAULT_HIGH_IP_REPUTATION_THREAT_SCORE_INCREMENT =
      "threat.management.config.service.ip.reputation.threat.score.increment.high";
  private static final String DEFAULT_MEDIUM_IP_REPUTATION_THREAT_SCORE_INCREMENT =
      "threat.management.config.service.ip.reputation.threat.score.increment.medium";
  private static final String DEFAULT_LOW_IP_REPUTATION_THREAT_SCORE_INCREMENT =
      "threat.management.config.service.ip.reputation.threat.score.increment.low";

  private final Config config;

  ThreatManagementConfigServiceConfig(Config config) {
    this.config = config;
  }

  public int getDefaultThreatUpperBoundLowScore() {
    return config.getInt(DEFAULT_THREAT_UPPER_BOUND_LOW_SCORE_KEY);
  }

  public int getDefaultThreatUpperBoundMediumScore() {
    return config.getInt(DEFAULT_THREAT_UPPER_BOUND_MEDIUM_SCORE_KEY);
  }

  public int getDefaultThreatUpperBoundHighScore() {
    return config.getInt(DEFAULT_THREAT_UPPER_BOUND_HIGH_SCORE_KEY);
  }

  public int getDefaultSecurityEventContributionLowScore() {
    return config.getInt(DEFAULT_SECURITY_EVENT_CONTRIBUTION_LOW_SCORE_KEY);
  }

  public int getDefaultAnomalyContributionScore() {
    return config.getInt(DEFAULT_ANOMALY_CONTRIBUTION_SCORE_KEY);
  }

  public int getDefaultSecurityEventContributionMediumScore() {
    return config.getInt(DEFAULT_SECURITY_EVENT_CONTRIBUTION_MEDIUM_SCORE_KEY);
  }

  public int getDefaultSecurityEventContributionHighScore() {
    return config.getInt(DEFAULT_SECURITY_EVENT_CONTRIBUTION_HIGH_SCORE_KEY);
  }

  public int getDefaultSecurityEventContributionCriticalScore() {
    return config.getInt(DEFAULT_SECURITY_EVENT_CONTRIBUTION_CRITICAL_SCORE_KEY);
  }

  public int getDefaultCriticalIpReputationThreatScoreIncrement() {
    return config.getInt(DEFAULT_CRITICAL_IP_REPUTATION_THREAT_SCORE_INCREMENT);
  }

  public int getDefaultHighIpReputationThreatScoreIncrement() {
    return config.getInt(DEFAULT_HIGH_IP_REPUTATION_THREAT_SCORE_INCREMENT);
  }

  public int getDefaultMediumIpReputationThreatScoreIncrement() {
    return config.getInt(DEFAULT_MEDIUM_IP_REPUTATION_THREAT_SCORE_INCREMENT);
  }

  public int getDefaultLowIpReputationThreatScoreIncrement() {
    return config.getInt(DEFAULT_LOW_IP_REPUTATION_THREAT_SCORE_INCREMENT);
  }
}
