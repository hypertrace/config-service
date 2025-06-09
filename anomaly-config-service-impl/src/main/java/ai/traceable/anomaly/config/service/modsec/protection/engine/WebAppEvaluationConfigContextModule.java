package ai.traceable.anomaly.config.service.modsec.protection.engine;

import com.google.inject.AbstractModule;

public class WebAppEvaluationConfigContextModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(WebAppEvaluationConfigContextManager.class)
        .to(WebAppEvaluationConfigContextManagerImpl.class);
  }
}
