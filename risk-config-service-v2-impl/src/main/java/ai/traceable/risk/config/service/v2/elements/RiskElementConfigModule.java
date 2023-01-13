package ai.traceable.risk.config.service.v2.elements;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskElementConfig;
import ai.traceable.risk.config.service.v2.elements.builder.RiskElementConfigBuilder;
import ai.traceable.risk.config.service.v2.elements.validator.RiskElementConfigValidator;
import ai.traceable.risk.config.service.v2.elements.validator.RiskElementConfigValidatorImpl;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

public class RiskElementConfigModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<RiskConfigBuilder<RiskElementConfig>>() {})
        .to(RiskElementConfigBuilder.class);
    bind(RiskElementConfigValidator.class).to(RiskElementConfigValidatorImpl.class);
  }
}
