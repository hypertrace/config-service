package ai.traceable.anomaly.config.service.global.ruleinfo;

import ai.traceable.anomaly.config.service.registry.AnomalyConfigRegistryModule;
import com.google.inject.AbstractModule;

public class RuleInfoModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(RuleInfoManager.class).to(AnomalyRuleInfoManagerImpl.class);
    install(new AnomalyConfigRegistryModule());
  }
}
