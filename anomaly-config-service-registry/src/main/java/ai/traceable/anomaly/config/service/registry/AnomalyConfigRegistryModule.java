package ai.traceable.anomaly.config.service.registry;

import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistryImpl;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistryImpl;
import com.google.inject.AbstractModule;

public class AnomalyConfigRegistryModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(ApiDefinitionRegistry.class).to(ApiDefinitionRegistryImpl.class);
    bind(ModsecRulesRegistry.class).to(ModsecRulesRegistryImpl.class);
    bind(SessionRulesRegistry.class).to(SessionRulesRegistryImpl.class);
  }
}
