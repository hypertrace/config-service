package ai.traceable.blocking.config.service.v2.regions;

import ai.traceable.blocking.config.service.common.regions.GenericRegionRuleAggregator;
import ai.traceable.blocking.config.service.v2.RegionBlockingRules;
import ai.traceable.blocking.config.service.v2.RegionIpBlockingRule;
import com.google.inject.Inject;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DefaultRegionBlockingManager implements RegionBlockingManager {
  private final GenericRegionRuleAggregator<RegionIpBlockingRule> regionRuleAggregator;

  @Inject
  public DefaultRegionBlockingManager(
      GenericRegionRuleAggregator<RegionIpBlockingRule> regionRuleAggregator) {
    this.regionRuleAggregator = regionRuleAggregator;
  }

  @Override
  public RegionBlockingRules getEnabledBlockingRules(
      RequestContext requestContext, Optional<String> environmentId) {
    return RegionBlockingRules.newBuilder()
        .addAllRegionIpBlockingRules(
            regionRuleAggregator.getEnabledBlockingRules(requestContext, environmentId))
        .build();
  }
}
