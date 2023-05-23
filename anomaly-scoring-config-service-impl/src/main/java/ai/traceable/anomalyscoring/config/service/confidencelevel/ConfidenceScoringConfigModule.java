package ai.traceable.anomalyscoring.config.service.confidencelevel;

import com.google.inject.AbstractModule;

public class ConfidenceScoringConfigModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ConfidenceScoringConfigManager.class).to(DefaultConfidenceScoringConfigManager.class);
  }
}
