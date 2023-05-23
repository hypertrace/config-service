package ai.traceable.anomalyscoring.config.service;

import com.typesafe.config.Config;

public class AnomalyScoringConfigServiceConfig {

  private static final String DEFAULT_ANOMALY_SCORING_IMPACT_LEVEL_MEDIUM_KEY =
      "anomaly.scoring.config.service.impact.level.score.mediumMinScore";
  private static final String DEFAULT_ANOMALY_SCORING_IMPACT_LEVEL_HIGH_KEY =
      "anomaly.scoring.config.service.impact.level.score.highMinScore";

  private static final String DEFAULT_ANOMALY_SCORING_CONFIDENCE_LEVEL_MEDIUM_KEY =
      "anomaly.scoring.config.service.confidence.level.score.mediumMinScore";
  private static final String DEFAULT_ANOMALY_SCORING_CONFIDENCE_LEVEL_HIGH_KEY =
      "anomaly.scoring.config.service.confidence.level.score.highMinScore";

  private final Config config;

  AnomalyScoringConfigServiceConfig(Config config) {
    this.config = config;
  }

  public int getDefaultMediumImpactMinScore() {
    return config.getInt(DEFAULT_ANOMALY_SCORING_IMPACT_LEVEL_MEDIUM_KEY);
  }

  public int getDefaultHighImpactMinScore() {
    return config.getInt(DEFAULT_ANOMALY_SCORING_IMPACT_LEVEL_HIGH_KEY);
  }

  public int getDefaultMediumConfidenceMinScore() {
    return config.getInt(DEFAULT_ANOMALY_SCORING_CONFIDENCE_LEVEL_MEDIUM_KEY);
  }

  public int getDefaultHighConfidenceMinScore() {
    return config.getInt(DEFAULT_ANOMALY_SCORING_CONFIDENCE_LEVEL_HIGH_KEY);
  }
}
