package ai.traceable.region.config.service.rules.migration;

import com.google.inject.AbstractModule;

public class RegionRulesMigrationModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(RegionRulesMigrationManager.class).to(RegionRulesMigrationManagerImpl.class);
  }
}
