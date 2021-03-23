package ai.traceable.blocking.config.service.regions;

import static com.google.common.collect.Streams.zip;

import ai.traceable.blocking.config.service.v1.BlockingRule;
import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.GetDetailedRegionsRequest;
import ai.traceable.region.config.service.v1.IpRange;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionsFilter;
import com.google.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

class RegionRuleConverter {
  private final RegionConfigServiceBlockingStub regionConfigServiceStub;
  private final ActionTypeConverter actionTypeConverter;
  private final IpBlockingConverter ipBlockingConverter;
  private final BlockingInfoConverter blockingInfoConverter;

  @Inject
  RegionRuleConverter(
      RegionConfigServiceBlockingStub regionConfigServiceStub,
      ActionTypeConverter actionTypeConverter,
      IpBlockingConverter ipBlockingConverter,
      BlockingInfoConverter blockingInfoConverter) {
    this.regionConfigServiceStub = regionConfigServiceStub;
    this.actionTypeConverter = actionTypeConverter;
    this.ipBlockingConverter = ipBlockingConverter;
    this.blockingInfoConverter = blockingInfoConverter;
  }

  List<BlockingRule> convert(List<RegionRule> regionRules) {
    Map<String, DetailedRegion> regions =
        getRegions(regionRules).stream()
            .collect(Collectors.toMap(DetailedRegion::getId, Function.identity()));

    return regionRules.stream()
        .map(regionRule -> this.convert(regionRule, regions))
        .flatMap(Optional::stream)
        .collect(Collectors.toList());
  }

  private Optional<BlockingRule> convert(
      RegionRule regionRule, Map<String, DetailedRegion> regions) {
    List<IpRange> ipRanges =
        regionRule.getRegionIdList().stream()
            .filter(regions::containsKey)
            .flatMap(regionId -> regions.get(regionId).getIpRangeList().stream())
            .collect(Collectors.toList());

    return zip(
            this.actionTypeConverter.convert(regionRule.getActionType()).stream(),
            this.blockingInfoConverter.convert(regionRule).stream(),
            (actionType, blockingInfo) ->
                BlockingRule.newBuilder()
                    .setActionType(actionType)
                    .setIpBlocking(this.ipBlockingConverter.convert(ipRanges))
                    .setBlockingInfo(blockingInfo)
                    .build())
        .findFirst();
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
