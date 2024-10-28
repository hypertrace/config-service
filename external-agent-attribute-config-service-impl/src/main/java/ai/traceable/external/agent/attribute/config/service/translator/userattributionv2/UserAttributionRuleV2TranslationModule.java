package ai.traceable.external.agent.attribute.config.service.translator.userattributionv2;

import com.google.inject.AbstractModule;

public class UserAttributionRuleV2TranslationModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(UserAttributionRuleV2Translator.class).to(UserAttributionRuleV2TranslatorImpl.class);
  }
}
