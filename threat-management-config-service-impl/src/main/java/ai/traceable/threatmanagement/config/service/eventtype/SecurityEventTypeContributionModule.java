package ai.traceable.threatmanagement.config.service.eventtype;

import com.google.inject.AbstractModule;

public class SecurityEventTypeContributionModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(SecurityEventTypeContributionManager.class)
        .to(DefaultSecurityEventTypeContributionManager.class);
  }
}
