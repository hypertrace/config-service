package ai.traceable.anomaly.config.service.registry;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefRulesRegistry;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistryImpl;
import com.google.inject.AbstractModule;

public class AnomalyConfigRegistryModule extends AbstractModule {

  AnomalyConfigRegistryModule() {}

  @Override
  protected void configure() {
    bind(ApiDefRulesRegistry.class).to(ApiDefRulesRegistryImpl.class);
    bind(ModsecRulesRegistry.class).to(ModsecRulesRegistryImpl.class);
    bind(SessionRulesRegistry.class).to(SessionRulesRegistryImpl.class);
  }
}
