package ai.traceable.anomaly.config.service.modsec.rules;

import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistryImpl;
import com.google.inject.AbstractModule;

public class ModsecModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(ModsecValidator.class).to(ModsecValidatorImpl.class);
    bind(ModsecManager.class).to(ModsecManagerImpl.class);
    bind(ModsecRulesRegistry.class).to(ModsecRulesRegistryImpl.class);
  }
}
