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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RegionRulesFetcher implements RulesFetcher {
  private final Clock clock;
  private final RegionConfigServiceBlockingStub regionConfigServiceStub;
  private final ClientConfig clientConfig;

  @Inject
  public RegionRulesFetcher(
      Clock clock,
      RegionConfigServiceBlockingStub regionConfigServiceStub,
      ClientConfig clientConfig) {
    this.clock = clock;
    this.regionConfigServiceStub = regionConfigServiceStub;
    this.clientConfig = clientConfig;
  }

  public List<RegionRule> fetchRegionRules(
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
    return requestContext.call(
        () ->
            this.regionConfigServiceStub
                .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .getAllRegionRules(rulesRequest)
                .getRuleList()
                .stream()
                .filter(rule -> isRuleActive(clock.millis(), rule))
                .collect(Collectors.toUnmodifiableList()));
  }

  public List<DetailedRegion> fetchDetailedRegions(List<String> countryIsoCodes) {
    List<RegionIdentifier> regionIdentifiers =
        countryIsoCodes.stream()
            .map(isoCode -> RegionIdentifier.newBuilder().setCountryIsoCode(isoCode).build())
            .collect(Collectors.toList());
    return this.regionConfigServiceStub
        .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
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
