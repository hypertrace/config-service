package ai.traceable.blocking.config.service.v1.regions;

import ai.traceable.blocking.config.service.common.regions.GenericRegionRuleAggregator;
import ai.traceable.blocking.config.service.v1.RegionBlockingRules;
import ai.traceable.blocking.config.service.v1.RegionIpBlockingRule;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DefaultRegionBlockingManager implements RegionBlockingManager {
  private final GenericRegionRuleAggregator<RegionIpBlockingRule> regionRuleAggregator;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultRegionBlockingManager(
      GenericRegionRuleAggregator<RegionIpBlockingRule> regionRuleAggregator,
      UuidGenerator uuidGenerator) {
    this.regionRuleAggregator = regionRuleAggregator;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public RegionBlockingRules getEnabledBlockingRules(
      RequestContext requestContext, String requestHash, Optional<String> environmentId) {
    List<RegionIpBlockingRule> regionIpBlockingRules =
        regionRuleAggregator.getEnabledBlockingRules(requestContext, environmentId);

    String responseHash = uuidGenerator.generateId(regionIpBlockingRules);
    RegionBlockingRules.Builder regionBlockingRulesBuilder =
        RegionBlockingRules.newBuilder().setHash(responseHash);
    if (!responseHash.equals(requestHash)) {
      regionBlockingRulesBuilder.addAllRegionIpBlockingRules(regionIpBlockingRules);
    }
    return regionBlockingRulesBuilder.build();
  }
}
