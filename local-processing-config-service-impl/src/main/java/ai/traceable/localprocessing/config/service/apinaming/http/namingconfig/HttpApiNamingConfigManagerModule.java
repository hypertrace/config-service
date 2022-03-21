package ai.traceable.localprocessing.config.service.apinaming.http.namingconfig;

import com.google.inject.AbstractModule;

public class HttpApiNamingConfigManagerModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(HttpApiNamingConfigManager.class).to(DefaultHttpApiNamingConfigManager.class);
  }
}
