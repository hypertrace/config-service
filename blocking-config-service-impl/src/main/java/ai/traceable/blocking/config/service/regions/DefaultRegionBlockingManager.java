package ai.traceable.blocking.config.service.regions;

import ai.traceable.blocking.config.service.UuidGenerator;
import ai.traceable.blocking.config.service.v1.RegionBlockingRules;
import ai.traceable.blocking.config.service.v1.RegionIpBlockingRule;
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
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultRegionBlockingManager(
      Clock clock,
      RegionConfigServiceBlockingStub regionConfigServiceStub,
      RegionBlockingRulesConverter ruleConverter,
      UuidGenerator uuidGenerator) {
    this.clock = clock;
    this.regionConfigServiceStub = regionConfigServiceStub;
    this.ruleConverter = ruleConverter;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public RegionBlockingRules getEnabledBlockingRules(String requestHash) {
    List<RegionRule> regionRules =
        this.regionConfigServiceStub
            .getAllRegionRules(GetAllRegionRulesRequest.getDefaultInstance())
            .getRuleList();

    List<RegionRule> activeRegionRules = getActiveRegionRules(regionRules);
    List<RegionIpBlockingRule> regionIpBlockingRules =
        this.ruleConverter.convert(activeRegionRules);

    String responseHash = uuidGenerator.generateId(regionIpBlockingRules);
    RegionBlockingRules.Builder regionBlockingRulesBuilder =
        RegionBlockingRules.newBuilder().setHash(responseHash);
    if (!responseHash.equals(requestHash)) {
      regionBlockingRulesBuilder.addAllRegionIpBlockingRules(regionIpBlockingRules);
    }
    return regionBlockingRulesBuilder.build();
  }

  private List<RegionRule> getActiveRegionRules(List<RegionRule> regionRules) {
    return regionRules.stream()
        .filter(rule -> isRuleActive(clock.millis(), rule.getExpirationMillis()))
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean isRuleActive(long currentTimeMillis, long expirationMillis) {
    return expirationMillis == 0 || expirationMillis > currentTimeMillis;
  }
}
