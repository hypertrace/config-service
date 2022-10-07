package ai.traceable.span.processing.config.service.apinamingrules;

import com.google.inject.AbstractModule;

public class ApiNamingRulesManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ApiNamingRulesManager.class).to(DefaultApiNamingRulesManager.class);
  }
}
