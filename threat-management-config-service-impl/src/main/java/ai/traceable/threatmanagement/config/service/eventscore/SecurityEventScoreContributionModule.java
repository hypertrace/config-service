package ai.traceable.threatmanagement.config.service.eventscore;

import com.google.inject.AbstractModule;

public class SecurityEventScoreContributionModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(SecurityEventScoreContributionManager.class)
        .to(DefaultSecurityEventScoreContributionManager.class);
  }
}
