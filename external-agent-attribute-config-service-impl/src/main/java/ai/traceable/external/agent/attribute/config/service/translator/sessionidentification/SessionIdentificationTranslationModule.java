package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location.BodyLocationTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location.CookieLocationTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location.HeaderLocationTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location.QueryParamLocationTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location.RequestLocationTranslator;
import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location.ResponseLocationTranslator;
import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

public class SessionIdentificationTranslationModule extends AbstractModule {

  @Override
  protected void configure() {
    Multibinder<RequestLocationTranslator> requestLocationTranslatorMultibinder =
        Multibinder.newSetBinder(binder(), RequestLocationTranslator.class);

    requestLocationTranslatorMultibinder.addBinding().to(HeaderLocationTranslator.class);
    requestLocationTranslatorMultibinder.addBinding().to(BodyLocationTranslator.class);
    requestLocationTranslatorMultibinder.addBinding().to(CookieLocationTranslator.class);
    requestLocationTranslatorMultibinder.addBinding().to(QueryParamLocationTranslator.class);

    Multibinder<ResponseLocationTranslator> responseLocationTranslatorMultibinder =
        Multibinder.newSetBinder(binder(), ResponseLocationTranslator.class);

    responseLocationTranslatorMultibinder.addBinding().to(HeaderLocationTranslator.class);
    responseLocationTranslatorMultibinder.addBinding().to(BodyLocationTranslator.class);
    responseLocationTranslatorMultibinder.addBinding().to(CookieLocationTranslator.class);
  }
}
