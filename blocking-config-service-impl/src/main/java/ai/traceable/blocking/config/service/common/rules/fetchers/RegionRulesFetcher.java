package ai.traceable.blocking.config.service.common.rules.fetchers;

import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.EnvironmentScope;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetDetailedRegionsRequest;
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionIdentifier;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionsFilter;
import ai.traceable.region.config.service.v1.RuleScope;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RegionRulesFetcher implements RulesFetcher {
  private final Clock clock;
  private final RegionConfigServiceBlockingStub regionConfigServiceStub;

  @Inject
  public RegionRulesFetcher(Clock clock, RegionConfigServiceBlockingStub regionConfigServiceStub) {
    this.clock = clock;
    this.regionConfigServiceStub = regionConfigServiceStub;
  }

  public List<DetailedRegion> fetchRegionRules(
      RequestContext requestContext, Optional<String> environmentId) {
    GetAllRegionRulesRequest rulesRequest =
        GetAllRegionRulesRequest.newBuilder()
            .setFilter(
                GetRegionRulesFilter.newBuilder()
                    .setDisabled(false)
                    .setRuleScope(
                        RuleScope.newBuilder()
                            .setEnvironmentScope(
                                environmentId
                                    .map(id -> EnvironmentScope.newBuilder().addEnvironmentIds(id))
                                    .orElse(EnvironmentScope.newBuilder()))))
            .build();
    List<RegionRule> regionRules =
        requestContext.call(
            () -> this.regionConfigServiceStub.getAllRegionRules(rulesRequest).getRuleList());

    Set<String> regionIds =
        regionRules.stream()
            .filter(rule -> isRuleActive(clock.millis(), rule))
            .flatMap(rule -> rule.getRegionIdList().stream())
            .collect(Collectors.toSet());
    if (regionIds.isEmpty()) {
      return Collections.emptyList();
    }

    return this.regionConfigServiceStub
        .getDetailedRegions(
            GetDetailedRegionsRequest.newBuilder()
                .setFilter(RegionsFilter.newBuilder().addAllId(regionIds))
                .build())
        .getRegionList();
  }

  public List<DetailedRegion> fetchDetailedRegions(List<String> countryIsoCodes) {
    List<RegionIdentifier> regionIdentifiers =
        countryIsoCodes.stream()
            .map(isoCode -> RegionIdentifier.newBuilder().setCountryIsoCode(isoCode).build())
            .collect(Collectors.toList());
    return this.regionConfigServiceStub
        .getDetailedRegions(
            GetDetailedRegionsRequest.newBuilder()
                .setFilter(RegionsFilter.newBuilder().addAllRegionIdentifier(regionIdentifiers))
                .build())
        .getRegionList();
  }

  private boolean isRuleActive(long currentTimeMillis, RegionRule regionRule) {
    if (!regionRule.hasExpirationDetails()) {
      return true;
    }
    long expirationMillis = regionRule.getExpirationDetails().getTimestampMillis();
    return expirationMillis == 0 || expirationMillis > currentTimeMillis;
  }
}
