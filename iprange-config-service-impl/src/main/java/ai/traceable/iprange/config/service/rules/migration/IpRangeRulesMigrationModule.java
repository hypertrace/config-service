package ai.traceable.iprange.config.service.rules.migration;

import com.google.inject.AbstractModule;

public class IpRangeRulesMigrationModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(IpRangeRulesMigrationManager.class).to(IpRangeRulesMigrationManagerImpl.class);
  }
}
