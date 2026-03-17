package ai.traceable.risk.config.service.v2.factors;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskFactorConfig;
import ai.traceable.risk.config.service.v2.factors.builder.RiskFactorConfigBuilder;
import ai.traceable.risk.config.service.v2.factors.comparator.RiskFactorConfigsComparator;
import ai.traceable.risk.config.service.v2.factors.comparator.RiskFactorConfigsComparatorImpl;
import ai.traceable.risk.config.service.v2.factors.manager.RiskFactorConfigsManager;
import ai.traceable.risk.config.service.v2.factors.manager.RiskFactorConfigsManagerImpl;
import ai.traceable.risk.config.service.v2.factors.validator.RiskFactorConfigsValidator;
import ai.traceable.risk.config.service.v2.factors.validator.RiskFactorConfigsValidatorImpl;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;

public class RiskFactorConfigsModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<RiskConfigBuilder<RiskFactorConfig>>() {})
        .to(RiskFactorConfigBuilder.class);
    bind(new TypeLiteral<IdentifiedObjectStore<RiskFactorConfig>>() {})
        .to(RiskFactorConfigStore.class);

    bind(RiskFactorConfigsManager.class).to(RiskFactorConfigsManagerImpl.class);
    bind(RiskFactorConfigsValidator.class).to(RiskFactorConfigsValidatorImpl.class);
    bind(RiskFactorConfigsComparator.class).to(RiskFactorConfigsComparatorImpl.class);
  }
}
