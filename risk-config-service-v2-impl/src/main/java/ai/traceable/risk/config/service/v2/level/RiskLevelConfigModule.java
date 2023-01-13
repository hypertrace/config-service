package ai.traceable.risk.config.service.v2.level;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskLevelConfigValues;
import ai.traceable.risk.config.service.v2.level.builder.RiskLevelConfigBuilder;
import ai.traceable.risk.config.service.v2.level.validator.RiskLevelConfigValidator;
import ai.traceable.risk.config.service.v2.level.validator.RiskLevelConfigValidatorImpl;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

public class RiskLevelConfigModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<RiskConfigBuilder<RiskLevelConfigValues>>() {})
        .to(RiskLevelConfigBuilder.class);

    bind(RiskLevelConfigValues.class).toProvider(DefaultRiskLevelConfigValuesProvider.class);

    bind(RiskLevelConfigValidator.class).to(RiskLevelConfigValidatorImpl.class);
  }
}
