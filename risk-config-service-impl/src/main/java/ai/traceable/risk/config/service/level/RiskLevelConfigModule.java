package ai.traceable.risk.config.service.level;

import ai.traceable.risk.config.service.level.processor.RiskLevelConfigManagerImpl;
import ai.traceable.risk.config.service.level.processor.RiskLevelConfigServiceDao;
import ai.traceable.risk.config.service.level.processor.RiskLevelConfigUtils;
import ai.traceable.risk.config.service.processor.RiskConfigServiceDao;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskLevelConfigValues;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

public class RiskLevelConfigModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<RiskConfigUtils<RiskLevelConfigValues>>() {})
        .to(RiskLevelConfigUtils.class);
    bind(new TypeLiteral<RiskConfigServiceDao<RiskLevelConfigValues>>() {})
        .to(RiskLevelConfigServiceDao.class);

    bind(RiskLevelConfigValues.class).toProvider(DefaultRiskLevelConfigValuesProvider.class);

    bind(RiskLevelConfigManager.class).to(RiskLevelConfigManagerImpl.class);
  }
}
