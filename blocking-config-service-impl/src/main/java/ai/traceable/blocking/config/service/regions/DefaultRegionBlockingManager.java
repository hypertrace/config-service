package ai.traceable.blocking.config.service.regions;

import ai.traceable.blocking.config.service.v1.RegionBlockingRules;
import ai.traceable.blocking.config.service.v1.RegionIpBlockingRule;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.region.config.service.v1.EnvironmentScope;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RuleScope;
import com.google.inject.Inject;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

class DefaultRegionBlockingManager implements RegionBlockingManager {
  private final Clock clock;
  private final RegionConfigServiceBlockingStub regionConfigServiceStub;
  private final RegionBlockingRulesConverter ruleConverter;
  private final UuidGenerator uuidGenerator;

  @Inject
  public DefaultRegionBlockingManager(
      Clock clock,
      RegionConfigServiceBlockingStub regionConfigServiceStub,
      RegionBlockingRulesConverter ruleConverter,
      UuidGenerator uuidGenerator) {
    this.clock = clock;
    this.regionConfigServiceStub = regionConfigServiceStub;
    this.ruleConverter = ruleConverter;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public RegionBlockingRules getEnabledBlockingRules(
      RequestContext requestContext, String requestHash, Optional<String> environmentId) {
    GetAllRegionRulesRequest rulesRequest =
        GetAllRegionRulesRequest.newBuilder()
            .setFilter(
                GetRegionRulesFilter.newBuilder()
                    .setDisabled(false)
                    .setRuleScope(
                        environmentId
                            .map(
                                id ->
                                    RuleScope.newBuilder()
                                        .setEnvironmentScope(
                                            EnvironmentScope.newBuilder().addEnvironmentIds(id))
                                        .build())
                            .orElse(RuleScope.getDefaultInstance())))
            .build();
    List<RegionRule> regionRules =
        requestContext.call(
            () -> this.regionConfigServiceStub.getAllRegionRules(rulesRequest).getRuleList());

    List<RegionRule> activeRegionRules = getActiveRegionRules(regionRules);
    List<RegionIpBlockingRule> regionIpBlockingRules =
        this.ruleConverter.convert(activeRegionRules);

    String responseHash = uuidGenerator.generateId(regionIpBlockingRules);
    RegionBlockingRules.Builder regionBlockingRulesBuilder =
        RegionBlockingRules.newBuilder().setHash(responseHash);
    if (!responseHash.equals(requestHash)) {
      regionBlockingRulesBuilder.addAllRegionIpBlockingRules(regionIpBlockingRules);
    }
    return regionBlockingRulesBuilder.build();
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
}
