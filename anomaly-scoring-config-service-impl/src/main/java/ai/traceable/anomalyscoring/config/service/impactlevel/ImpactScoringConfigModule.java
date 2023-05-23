package ai.traceable.anomalyscoring.config.service.impactlevel;

import com.google.inject.AbstractModule;

public class ImpactScoringConfigModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ImpactScoringConfigManager.class).to(DefaultImpactScoringConfigManager.class);
  }
}
