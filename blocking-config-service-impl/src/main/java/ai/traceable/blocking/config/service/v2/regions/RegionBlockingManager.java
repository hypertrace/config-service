package ai.traceable.blocking.config.service.v2.regions;

import ai.traceable.blocking.config.service.common.regions.GenericRegionRuleAggregator;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.RegionBlockingRules;
import ai.traceable.blocking.config.service.v2.RegionIpBlockingRule;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

public class RegionBlockingManager implements BlockingConfigManagerBase {
  private final GenericRegionRuleAggregator<RegionIpBlockingRule> regionRuleAggregator;
  private final UuidGenerator uuidGenerator;

  @Inject
  public RegionBlockingManager(
      GenericRegionRuleAggregator<RegionIpBlockingRule> regionRuleAggregator,
      UuidGenerator uuidGenerator) {
    this.regionRuleAggregator = regionRuleAggregator;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public List<BlockingConfigResponseElement> generateBlockingElements(
      List<BlockingConfigRequestElement> requestElements,
      BlockingRulesSupplier blockingRulesSupplier) {
    List<BlockingConfigRequestElement> regionRequestElements =
        requestElements.stream()
            .filter(BlockingConfigRequestElement::hasRegionBlockingRulesRequest)
            .collect(Collectors.toUnmodifiableList());
    if (regionRequestElements.isEmpty()) {
      return List.of();
    }

    RegionBlockingRules regionBlockingRules =
        RegionBlockingRules.newBuilder()
            .addAllRegionIpBlockingRules(
                regionRuleAggregator.getEnabledBlockingRules(
                    blockingRulesSupplier.getRequestContext(),
                    blockingRulesSupplier.getEnvironmentId()))
            .build();
    return List.of(buildResponseElement(regionRequestElements, regionBlockingRules));
  }

  private BlockingConfigResponseElement buildResponseElement(
      List<BlockingConfigRequestElement> requestElements, RegionBlockingRules regionBlockingRules) {
    String responseHash = uuidGenerator.generateId(regionBlockingRules);
    if (requestElements.stream()
        .map(BlockingConfigRequestElement::getPreviousHash)
        .allMatch(responseHash::equals)) {
      return BlockingConfigResponseElement.newBuilder()
          .setHash(responseHash)
          .setRegionBlockingRules(RegionBlockingRules.getDefaultInstance())
          .build();
    }
    return BlockingConfigResponseElement.newBuilder()
        .setHash(responseHash)
        .setRegionBlockingRules(regionBlockingRules)
        .build();
  }
}
