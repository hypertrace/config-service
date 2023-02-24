package ai.traceable.blocking.config.service.v2.regions;

import ai.traceable.blocking.config.service.common.regions.GenericRegionRuleAggregator;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.RegionBlockingRules;
import ai.traceable.blocking.config.service.v2.RegionIpBlockingRule;
import ai.traceable.config.utils.UuidGenerator;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

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
      RequestContext requestContext,
      Optional<String> environmentId) {
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
                regionRuleAggregator.getEnabledBlockingRules(requestContext, environmentId))
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
