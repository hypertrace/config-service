package ai.traceable.blocking.config.service.v2.regions;

import ai.traceable.blocking.config.service.common.regions.GenericRegionRuleAggregator.GenericRegionRuleConverter;
import ai.traceable.blocking.config.service.v2.RegionIpBlockingRule;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

public class RegionBlockingManagerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(RegionBlockingManager.class).to(DefaultRegionBlockingManager.class);
    bind(new TypeLiteral<GenericRegionRuleConverter<RegionIpBlockingRule>>() {})
        .to(RegionIpRulesConverter.class);
  }
}
