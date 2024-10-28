package ai.traceable.config.service.rest.response;

import com.google.inject.AbstractModule;

public class ResponseConverterModule extends AbstractModule {

  @Override
  protected void configure() {
    /*
    need to explicitly bind classes, since HK2 doesn't automatically construct empty constructor
    for non-annotated classes
    */
    bind(ResponseConverter.class);
    bind(SimpleResponseConverter.class);
    bind(RedirectResponseConverter.class);
    bind(CookiesBuilder.class);
    bind(HeadersBuilder.class);
  }
}
