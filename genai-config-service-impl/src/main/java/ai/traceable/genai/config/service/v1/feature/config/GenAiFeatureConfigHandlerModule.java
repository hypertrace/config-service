package ai.traceable.genai.config.service.v1.feature.config;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;

public class GenAiFeatureConfigHandlerModule extends AbstractModule {

  @Override
  protected void configure() {
    Multibinder<GenAiFeatureConfigHandler<?>> handlerBinder =
        Multibinder.newSetBinder(binder(), new TypeLiteral<GenAiFeatureConfigHandler<?>>() {});
    handlerBinder.addBinding().to(IssuesSummaryFeatureConfigHandler.class);
    handlerBinder.addBinding().to(ThreatActivitySummaryFeatureConfigHandler.class);
  }
}
