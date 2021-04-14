package ai.traceable.blocking.config.service.regions;

import ai.traceable.blocking.config.service.v1.RegionBlockingRules;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import com.google.inject.Inject;
import java.time.Clock;
import java.util.List;
import java.util.stream.Collectors;

class DefaultRegionBlockingManager implements RegionBlockingManager {
  private final Clock clock;
  private final RegionConfigServiceBlockingStub regionConfigServiceStub;
  private final RegionBlockingRulesConverter ruleConverter;

  @Inject
  DefaultRegionBlockingManager(
      Clock clock,
      RegionConfigServiceBlockingStub regionConfigServiceStub,
      RegionBlockingRulesConverter ruleConverter) {
    this.clock = clock;
    this.regionConfigServiceStub = regionConfigServiceStub;
    this.ruleConverter = ruleConverter;
  }

  @Override
  public RegionBlockingRules getBlockingRules() {
    List<RegionRule> regionRules =
        this.regionConfigServiceStub
            .getAllRegionRules(GetAllRegionRulesRequest.getDefaultInstance())
            .getRuleList();

    List<RegionRule> activeRegionRules = getActiveRegionRules(regionRules);
    return this.ruleConverter.convert(activeRegionRules);
  }

  private List<RegionRule> getActiveRegionRules(List<RegionRule> regionRules) {
    return regionRules.stream()
        .filter(rule -> isRuleActive(clock.millis(), rule.getExpirationMillis()))
        .collect(Collectors.toList());
  }

  private boolean isRuleActive(long currentTimeMillis, long expirationMillis) {
    return expirationMillis == 0 || expirationMillis > currentTimeMillis;
  }
}
