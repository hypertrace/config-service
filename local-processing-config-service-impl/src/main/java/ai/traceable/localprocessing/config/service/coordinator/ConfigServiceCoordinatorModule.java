package ai.traceable.localprocessing.config.service.coordinator;

import com.google.inject.AbstractModule;

public class ConfigServiceCoordinatorModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(ConfigServiceCoordinator.class).to(ConfigServiceCoordinatorImpl.class);
  }
}
