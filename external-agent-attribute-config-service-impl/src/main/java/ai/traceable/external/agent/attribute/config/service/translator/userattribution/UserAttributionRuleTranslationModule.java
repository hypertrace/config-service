package ai.traceable.external.agent.attribute.config.service.translator.userattribution;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

public class UserAttributionRuleTranslationModule extends AbstractModule {

  @Override
  protected void configure() {
    Multibinder<UserAttributionRuleTranslator> multibinder =
        Multibinder.newSetBinder(binder(), UserAttributionRuleTranslator.class);
    multibinder.addBinding().to(BasicAuthRuleTranslator.class);
    multibinder.addBinding().to(CustomJsonRuleTranslator.class);
    multibinder.addBinding().to(CustomTokenRuleTranslator.class);
    multibinder.addBinding().to(NoOpYamlRuleTranslator.class);
    multibinder.addBinding().to(JwtRuleTranslator.class);
    multibinder.addBinding().to(RequestHeaderRuleTranslator.class);
    multibinder.addBinding().to(ResponseBodyRuleTranslator.class);
  }
}
