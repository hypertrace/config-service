package ai.traceable.anomaly.config.service.override.exclusion;

import com.google.inject.AbstractModule;

public class ExclusionRulesManagerModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(ExclusionRulesManager.class).to(DetectionOverrideExclusionRulesManager.class);
    bind(ExclusionRulesValidator.class).to(DetectionOverrideExclusionRulesValidator.class);
  }
}
