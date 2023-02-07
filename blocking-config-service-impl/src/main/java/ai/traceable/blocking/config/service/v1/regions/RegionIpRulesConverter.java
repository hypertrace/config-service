package ai.traceable.blocking.config.service.v1.regions;

import ai.traceable.blocking.config.service.common.regions.GenericRegionRuleAggregator.GenericRegionRuleConverter;
import ai.traceable.blocking.config.service.v1.RegionIpBlockingRule;
import ai.traceable.region.config.service.v1.DetailedRegion;
import com.google.inject.Inject;

class RegionIpRulesConverter implements GenericRegionRuleConverter<RegionIpBlockingRule> {
  private final IpRangesConverter ipRangesConverter;

  @Inject
  RegionIpRulesConverter(IpRangesConverter ipRangesConverter) {
    this.ipRangesConverter = ipRangesConverter;
  }

  public RegionIpBlockingRule convert(DetailedRegion detailedRegion) {
    return RegionIpBlockingRule.newBuilder()
        .setRegionId(detailedRegion.getId())
        .addAllIpRanges(ipRangesConverter.convert(detailedRegion.getIpRangeList()))
        .build();
  }
}
