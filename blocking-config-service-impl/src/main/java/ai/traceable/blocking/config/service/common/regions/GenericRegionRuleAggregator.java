package ai.traceable.blocking.config.service.common.regions;

import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.EnvironmentScope;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetDetailedRegionsRequest;
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionsFilter;
import ai.traceable.region.config.service.v1.RuleScope;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class GenericRegionRuleAggregator<T> {
  private final Clock clock;
  private final RegionConfigServiceBlockingStub regionConfigServiceStub;
  private final GenericRegionRuleConverter<T> ruleConverter;
  private final ClientConfig clientConfig;

  @Inject
  public GenericRegionRuleAggregator(
      Clock clock,
      RegionConfigServiceBlockingStub regionConfigServiceStub,
      GenericRegionRuleConverter<T> converter,
      ClientConfig clientConfig) {
    this.clock = clock;
    this.regionConfigServiceStub = regionConfigServiceStub;
    this.ruleConverter = converter;
    this.clientConfig = clientConfig;
  }

  public List<T> getEnabledBlockingRules(
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
            () ->
                this.regionConfigServiceStub
                    .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                    .getAllRegionRules(rulesRequest)
                    .getRuleList());

    List<RegionRule> activeRegionRules = getActiveRegionRules(regionRules);
    List<DetailedRegion> detailedRegions = getRegions(activeRegionRules);

    return detailedRegions.stream()
        .map(ruleConverter::convert)
        .collect(Collectors.toUnmodifiableList());
  }

  private List<RegionRule> getActiveRegionRules(List<RegionRule> regionRules) {
    return regionRules.stream()
        .filter(rule -> isRuleActive(clock.millis(), rule))
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean isRuleActive(long currentTimeMillis, RegionRule regionRule) {
    if (!regionRule.hasExpirationDetails()) {
      return true;
    }
    long expirationMillis = regionRule.getExpirationDetails().getTimestampMillis();
    return expirationMillis == 0 || expirationMillis > currentTimeMillis;
  }

  private List<DetailedRegion> getRegions(List<RegionRule> rules) {
    Set<String> regionIds =
        rules.stream().flatMap(rule -> rule.getRegionIdList().stream()).collect(Collectors.toSet());
    if (regionIds.isEmpty()) {
      return Collections.emptyList();
    }

    return this.regionConfigServiceStub
        .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
        .getDetailedRegions(
            GetDetailedRegionsRequest.newBuilder()
                .setFilter(RegionsFilter.newBuilder().addAllId(regionIds))
                .build())
        .getRegionList();
  }

  public interface GenericRegionRuleConverter<T> {
    T convert(DetailedRegion detailedRegion);
  }
}
