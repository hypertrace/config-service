package ai.traceable.threatmanagement.config.service.anomalyscore;

import com.google.inject.AbstractModule;

public class AnomalyScoreContributionModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(AnomalyScoreContributionManager.class).to(DefaultAnomalyScoreContributionManager.class);
  }
}
