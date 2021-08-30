package ai.traceable.customsignature.config.service.modsec;

import ai.traceable.customsignature.config.service.modsec.directives.ModsecDirectivesModule;
import com.google.inject.AbstractModule;

public class ModsecRulesManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ModsecRulesManager.class).to(CustomSignatureModsecRulesManager.class);
    install(new ModsecDirectivesModule());
  }
}
