package ai.traceable.span.processing.config.service.samplingconfigs;

import com.google.inject.AbstractModule;

public class SamplingConfigManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(SamplingConfigManager.class).to(DefaultSamplingConfigManager.class);
  }
}
