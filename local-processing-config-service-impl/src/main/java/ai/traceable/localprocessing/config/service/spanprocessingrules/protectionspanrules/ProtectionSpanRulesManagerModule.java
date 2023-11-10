package ai.traceable.localprocessing.config.service.spanprocessingrules.protectionspanrules;

import com.google.inject.AbstractModule;

public class ProtectionSpanRulesManagerModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(ProtectionSpanRulesManager.class).to(DefaultProtectionSpanRulesManager.class);
  }
}
