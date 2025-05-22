package ai.traceable.anomaly.config.service.global.version;

import ai.traceable.protection.rules.apiprotect.v1.ApiProtectionRulesProviderModule;
import ai.traceable.protection.rules.webapp.v1.WebAppProtectionRulesProviderModule;
import com.google.inject.AbstractModule;

public class RuleVersionModule extends AbstractModule {

  @Override
  protected void configure() {
    install(new ApiProtectionRulesProviderModule());
    install(new WebAppProtectionRulesProviderModule());
    bind(RuleVersionManager.class).to(RuleVersionManagerImpl.class);
  }
}
