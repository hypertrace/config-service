package ai.traceable.detection.exclusion.config.service.v1.rules.edge.decision.condition;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

public class DetectionExclusionRuleConditionModule extends AbstractModule {

  @Override
  protected void configure() {
    Multibinder<DetectionExclusionRuleConditionConverter> multiBinder =
        Multibinder.newSetBinder(binder(), DetectionExclusionRuleConditionConverter.class);
    multiBinder.addBinding().to(DetectionExclusionRuleIpTypeConditionConverter.class);
  }
}
