package ai.traceable.risk.config.service.v2.grid;

import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskScoringGridConfigValues;
import ai.traceable.risk.config.service.v2.grid.builder.RiskScoringGridConfigBuilder;
import ai.traceable.risk.config.service.v2.grid.comparator.RiskScoringGridConfigComparator;
import ai.traceable.risk.config.service.v2.grid.comparator.RiskScoringGridConfigComparatorImpl;
import ai.traceable.risk.config.service.v2.grid.manager.RiskScoringGridConfigManager;
import ai.traceable.risk.config.service.v2.grid.manager.RiskScoringGridConfigManagerImpl;
import ai.traceable.risk.config.service.v2.grid.validator.RiskScoringGridConfigValidator;
import ai.traceable.risk.config.service.v2.grid.validator.RiskScoringGridConfigValidatorImpl;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;

public class RiskScoringGridConfigModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<RiskConfigBuilder<RiskScoringGridConfigValues>>() {})
        .to(RiskScoringGridConfigBuilder.class);
    bind(new TypeLiteral<IdentifiedObjectStore<RiskScoringGridConfigValues>>() {})
        .to(RiskScoringGridConfigStore.class);

    bind(RiskScoringGridConfigValues.class)
        .toProvider(DefaultRiskScoringGridConfigValuesProvider.class);

    bind(RiskScoringGridConfigManager.class).to(RiskScoringGridConfigManagerImpl.class);
    bind(RiskScoringGridConfigValidator.class).to(RiskScoringGridConfigValidatorImpl.class);
    bind(RiskScoringGridConfigComparator.class).to(RiskScoringGridConfigComparatorImpl.class);
  }
}
