package ai.traceable.anomaly.config.service.global.ruleinfo;

import ai.traceable.protection.rules.aiapp.v1.AiAppRulesProviderModule;
import com.google.inject.AbstractModule;

public class RuleInfoModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(RuleInfoManager.class).to(AnomalyRuleInfoManagerImpl.class);
    bind(WebAppRuleInfoProvider.class).to(WebAppRuleInfoProviderImpl.class);
    bind(AiAppRuleInfoProvider.class).to(AiAppRuleInfoProviderImpl.class);
    install(new AiAppRulesProviderModule());
  }
}
