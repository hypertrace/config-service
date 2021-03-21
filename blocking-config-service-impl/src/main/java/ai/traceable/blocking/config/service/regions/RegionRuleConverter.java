package ai.traceable.blocking.config.service.regions;

import ai.traceable.blocking.config.service.v1.BlockingRule;
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.GetDetailedRegionsRequest;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionsFilter;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

class RegionRuleConverter {
  private final RegionConfigServiceBlockingStub regionConfigServiceStub;

  @Inject
  RegionRuleConverter(RegionConfigServiceBlockingStub regionConfigServiceStub) {
    this.regionConfigServiceStub = regionConfigServiceStub;
  }

  List<BlockingRule> convert(List<RegionRule> regionRules) {
    Map<String, DetailedRegion> regions =
        getRegions(regionRules).stream()
            .collect(Collectors.toMap(DetailedRegion::getId, Function.identity()));
    // TODO: Add implementation
    return Collections.emptyList();
  }

  private List<DetailedRegion> getRegions(List<RegionRule> rules) {
    Set<String> regionIds =
        rules.stream().flatMap(rule -> rule.getRegionIdList().stream()).collect(Collectors.toSet());

    return this.regionConfigServiceStub
        .getDetailedRegions(
            GetDetailedRegionsRequest.newBuilder()
                .setFilter(RegionsFilter.newBuilder().addAllId(regionIds).build())
                .build())
        .getRegionList();
  }
}
