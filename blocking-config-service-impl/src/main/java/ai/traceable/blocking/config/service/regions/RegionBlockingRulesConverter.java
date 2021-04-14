package ai.traceable.blocking.config.service.regions;

import ai.traceable.blocking.config.service.v1.RegionBlockingRules;
import ai.traceable.blocking.config.service.v1.RegionIpBlockingDetails;
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.GetDetailedRegionsRequest;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionsFilter;
import com.google.inject.Inject;
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

  RegionBlockingRules convert(List<RegionRule> regionRules) {
    return RegionBlockingRules.newBuilder()
        .addAllRegionIpBlockingDetails(
            getRegions(regionRules).stream()
                .map(detailedRegion -> convert(detailedRegion))
                .collect(Collectors.toList()))
        .build();
  }

  private RegionIpBlockingDetails convert(DetailedRegion detailedRegion) {
    return RegionIpBlockingDetails.newBuilder()
        .setRegionId(detailedRegion.getId())
        .addAllIpRanges(ipRangesConverter.convert(detailedRegion.getIpRangeList()))
        .build();
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
