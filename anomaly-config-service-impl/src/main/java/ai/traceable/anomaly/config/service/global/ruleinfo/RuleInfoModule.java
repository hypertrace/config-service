package ai.traceable.anomaly.config.service.global.ruleinfo;

import com.google.inject.AbstractModule;

public class RuleInfoModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(RuleInfoManager.class).to(AnomalyRuleInfoManagerImpl.class);
  }
}
