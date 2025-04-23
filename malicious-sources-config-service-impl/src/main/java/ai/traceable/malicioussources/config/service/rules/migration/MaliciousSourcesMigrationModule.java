package ai.traceable.malicioussources.config.service.rules.migration;

import com.google.inject.AbstractModule;

public class MaliciousSourcesMigrationModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(MaliciousSourcesMigrationManager.class).to(MaliciousSourcesMigrationManagerImpl.class);
  }
}
