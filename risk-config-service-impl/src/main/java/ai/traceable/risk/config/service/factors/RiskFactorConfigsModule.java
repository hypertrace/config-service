package ai.traceable.risk.config.service.factors;

import static ai.traceable.risk.config.service.factors.RiskFactorConfigsManager.IMPACT_ANNOTATION;
import static ai.traceable.risk.config.service.factors.RiskFactorConfigsManager.LIKELIHOOD_ANNOTATION;

import ai.traceable.risk.config.service.factors.processor.RiskElementConfigStore;
import ai.traceable.risk.config.service.factors.processor.RiskFactorConfigStore;
import ai.traceable.risk.config.service.factors.processor.RiskFactorConfigsManagerImpl;
import ai.traceable.risk.config.service.factors.processor.utils.RiskContributorConfigUtils;
import ai.traceable.risk.config.service.factors.processor.utils.RiskElementConfigUtils;
import ai.traceable.risk.config.service.factors.processor.utils.RiskFactorConfigUtils;
import ai.traceable.risk.config.service.processor.RiskConfigUtils;
import ai.traceable.risk.config.service.v1.RiskContributorConfigs;
import ai.traceable.risk.config.service.v1.RiskElementConfig;
import ai.traceable.risk.config.service.v1.RiskFactorConfig;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;

public class RiskFactorConfigsModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<RiskConfigUtils<RiskFactorConfig>>() {}).to(RiskFactorConfigUtils.class);
    bind(new TypeLiteral<RiskConfigUtils<RiskElementConfig>>() {}).to(RiskElementConfigUtils.class);
    bind(new TypeLiteral<RiskConfigUtils<RiskContributorConfigs>>() {})
        .to(RiskContributorConfigUtils.class);

    bind(new TypeLiteral<IdentifiedObjectStore<RiskElementConfig>>() {})
        .to(RiskElementConfigStore.class);
    bind(new TypeLiteral<IdentifiedObjectStore<RiskFactorConfig>>() {})
        .to(RiskFactorConfigStore.class);

    bind(RiskContributorConfigs.class)
        .annotatedWith(Names.named(LIKELIHOOD_ANNOTATION))
        .toProvider(DefaultRiskLikelihoodConfigsProvider.class);
    bind(RiskContributorConfigs.class)
        .annotatedWith(Names.named(IMPACT_ANNOTATION))
        .toProvider(DefaultRiskImpactConfigsProvider.class);

    bind(RiskFactorConfigsManager.class).to(RiskFactorConfigsManagerImpl.class);
  }
}
