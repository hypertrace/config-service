package ai.traceable.risk.config.service.v2.contributors;

import ai.traceable.risk.config.service.v2.EntityType;
import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.contributors.builder.RiskContributorConfigBuilder;
import ai.traceable.risk.config.service.v2.contributors.validator.RiskContributorConfigsValidator;
import ai.traceable.risk.config.service.v2.contributors.validator.RiskContributorConfigsValidatorImpl;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import java.util.Map;

public class RiskContributorConfigsModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<RiskConfigBuilder<RiskContributorConfigs>>() {})
        .to(RiskContributorConfigBuilder.class);
    bind(new TypeLiteral<Map<EntityType, RiskContributorConfigs>>() {})
        .toProvider(DefaultRiskContributorConfigsProvider.class);
    bind(RiskContributorConfigsValidator.class).to(RiskContributorConfigsValidatorImpl.class);
  }
}
