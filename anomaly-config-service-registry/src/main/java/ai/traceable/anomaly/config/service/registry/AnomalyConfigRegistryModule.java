package ai.traceable.anomaly.config.service.registry;

import ai.traceable.anomaly.config.service.registry.accounttakeover.AccountTakeoverRulesRegistry;
import ai.traceable.anomaly.config.service.registry.accounttakeover.AccountTakeoverRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistry;
import ai.traceable.anomaly.config.service.registry.apidef.ApiDefinitionRegistryImpl;
import ai.traceable.anomaly.config.service.registry.credentialstuffing.CredentialStuffingRulesRegistry;
import ai.traceable.anomaly.config.service.registry.credentialstuffing.CredentialStuffingRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.genai.GenAiRulesRegistry;
import ai.traceable.anomaly.config.service.registry.genai.GenAiRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistry;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistry;
import ai.traceable.anomaly.config.service.registry.session.SessionRulesRegistryImpl;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistry;
import ai.traceable.anomaly.config.service.registry.volumetric.VolumetricRulesRegistryImpl;
import com.google.inject.AbstractModule;

public class AnomalyConfigRegistryModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(ApiDefinitionRegistry.class).to(ApiDefinitionRegistryImpl.class);
    bind(ModsecRulesRegistry.class).to(ModsecRulesRegistryImpl.class);
    bind(SessionRulesRegistry.class).to(SessionRulesRegistryImpl.class);
    bind(VolumetricRulesRegistry.class).to(VolumetricRulesRegistryImpl.class);
    bind(CredentialStuffingRulesRegistry.class).to(CredentialStuffingRulesRegistryImpl.class);
    bind(AccountTakeoverRulesRegistry.class).to(AccountTakeoverRulesRegistryImpl.class);
    bind(GenAiRulesRegistry.class).to(GenAiRulesRegistryImpl.class);
  }
}
