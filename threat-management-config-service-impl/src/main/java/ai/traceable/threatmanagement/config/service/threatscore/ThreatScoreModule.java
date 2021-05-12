package ai.traceable.threatmanagement.config.service.threatscore;

import com.google.inject.AbstractModule;

public class ThreatScoreModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ThreatScoreManager.class).to(DefaultThreatScoreManager.class);
  }
}
