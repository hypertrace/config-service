package ai.traceable.blocking.config.service.regions;

import ai.traceable.blocking.config.service.v1.RegionIpBlockingRule;
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.GetDetailedRegionsRequest;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionsFilter;
import com.google.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class RegionBlockingRulesConverter {
  private final RegionConfigServiceBlockingStub regionConfigServiceStub;
  private final IpRangesConverter ipRangesConverter;

  @Inject
  RegionBlockingRulesConverter(
      RegionConfigServiceBlockingStub regionConfigServiceStub,
      IpRangesConverter ipRangesConverter) {
    this.regionConfigServiceStub = regionConfigServiceStub;
    this.ipRangesConverter = ipRangesConverter;
  }

  List<RegionIpBlockingRule> convert(List<RegionRule> regionRules) {
    return getRegions(regionRules).stream()
        .map(detailedRegion -> convert(detailedRegion))
        .collect(Collectors.toUnmodifiableList());
  }

  private RegionIpBlockingRule convert(DetailedRegion detailedRegion) {
    return RegionIpBlockingRule.newBuilder()
        .setRegionId(detailedRegion.getId())
        .addAllIpRanges(ipRangesConverter.convert(detailedRegion.getIpRangeList()))
        .build();
  }

  private List<DetailedRegion> getRegions(List<RegionRule> rules) {
    Set<String> regionIds =
        rules.stream().flatMap(rule -> rule.getRegionIdList().stream()).collect(Collectors.toSet());
    if (regionIds.isEmpty()) {
      return Collections.emptyList();
    }

    return this.regionConfigServiceStub
        .getDetailedRegions(
            GetDetailedRegionsRequest.newBuilder()
                .setFilter(RegionsFilter.newBuilder().addAllId(regionIds).build())
                .build())
        .getRegionList();
  }
}
