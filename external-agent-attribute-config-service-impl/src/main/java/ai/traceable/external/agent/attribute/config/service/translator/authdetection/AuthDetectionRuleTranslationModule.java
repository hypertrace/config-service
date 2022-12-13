package ai.traceable.external.agent.attribute.config.service.translator.authdetection;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

public class AuthDetectionRuleTranslationModule extends AbstractModule {

  @Override
  protected void configure() {
    Multibinder<AuthDetectionRuleTranslator> multibinder =
        Multibinder.newSetBinder(binder(), AuthDetectionRuleTranslator.class);
    multibinder.addBinding().to(CompositeAuthDetectionRuleTranslator.class);
    multibinder.addBinding().to(CookieAuthDetectionRuleTranslator.class);
    multibinder.addBinding().to(FormBodyAuthDetectionRuleTranslator.class);
    multibinder.addBinding().to(HeaderAuthDetectionRuleTranslator.class);
    multibinder.addBinding().to(JsonBodyDetectionRuleTranslator.class);
    multibinder.addBinding().to(QueryAuthDetectionRuleTranslator.class);
  }
}
