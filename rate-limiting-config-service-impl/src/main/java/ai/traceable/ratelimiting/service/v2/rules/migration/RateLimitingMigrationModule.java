package ai.traceable.ratelimiting.service.v2.rules.migration;

import com.google.inject.AbstractModule;

public class RateLimitingMigrationModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(RateLimitingMigrationManager.class).to(RateLimitingMigrationManagerImpl.class);
  }
}
