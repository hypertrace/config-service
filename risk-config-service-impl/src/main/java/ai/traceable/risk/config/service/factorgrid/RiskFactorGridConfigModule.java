package ai.traceable.risk.config.service.factorgrid;

import ai.traceable.risk.config.service.factorgrid.processor.RiskFactorGridConfigManagerImpl;
import ai.traceable.risk.config.service.factorgrid.processor.RiskFactorGridConfigStore;
import ai.traceable.risk.config.service.factorgrid.processor.RiskFactorGridConfigUtils;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskFactorGridConfigValues;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import org.hypertrace.config.objectstore.DefaultObjectStore;

public class RiskFactorGridConfigModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<RiskConfigUtils<RiskFactorGridConfigValues>>() {})
        .to(RiskFactorGridConfigUtils.class);
    bind(new TypeLiteral<DefaultObjectStore<RiskFactorGridConfigValues>>() {})
        .to(RiskFactorGridConfigStore.class);

    bind(RiskFactorGridConfigValues.class)
        .toProvider(DefaultRiskFactorGridConfigValuesProvider.class);

    bind(RiskFactorGridConfigManager.class).to(RiskFactorGridConfigManagerImpl.class);
  }
}
