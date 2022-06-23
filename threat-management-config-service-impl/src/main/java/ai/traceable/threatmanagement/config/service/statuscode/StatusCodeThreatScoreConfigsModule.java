package ai.traceable.threatmanagement.config.service.statuscode;

import com.google.inject.AbstractModule;

public class StatusCodeThreatScoreConfigsModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(StatusCodeThreatScoreConfigsManager.class)
        .to(StatusCodeThreatScoreConfigsManagerImpl.class);
  }
}
