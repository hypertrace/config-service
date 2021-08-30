package ai.traceable.customsignature.config.service.modsec.directives;

import ai.traceable.anomaly.config.service.registry.AnomalyConfigRegistryModule;
import com.google.inject.AbstractModule;

public class ModsecDirectivesModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(ModsecDirectivesManager.class).to(ModsecDirectivesManagerImpl.class);
    install(new AnomalyConfigRegistryModule());
  }
}
