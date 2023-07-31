package ai.traceable.blocking.config.service.common.rules.fetchers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.region.config.service.v1.DetailedRegion;
import ai.traceable.region.config.service.v1.EnvironmentScope;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetAllRegionRulesResponse;
import ai.traceable.region.config.service.v1.GetDetailedRegionsRequest;
import ai.traceable.region.config.service.v1.GetDetailedRegionsResponse;
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RegionsFilter;
import ai.traceable.region.config.service.v1.RuleScope;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RegionRulesFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String environmentId = "env-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final Long TIMESTAMP = 10000L;

  private Clock clock;
  private RegionConfigServiceBlockingStub regionConfigServiceStub;
  private RegionRulesFetcher regionRulesFetcher;

  @BeforeEach
  void setup() {
    clock = mock(Clock.class);
    regionConfigServiceStub = mock(RegionConfigServiceBlockingStub.class);
    regionRulesFetcher = new RegionRulesFetcher(clock, regionConfigServiceStub);

    when(this.clock.millis()).thenReturn(TIMESTAMP);

    // Mock response for detailed rules fetch
    doAnswer(
            invocation -> {
              RegionsFilter filter =
                  invocation.getArgument(0, GetDetailedRegionsRequest.class).getFilter();
              return GetDetailedRegionsResponse.newBuilder()
                  .addAllRegion(
                      filter.getIdList().stream()
                          .map(REGIONS_MAP::get)
                          .collect(Collectors.toList()))
                  .build();
            })
        .when(regionConfigServiceStub)
        .getDetailedRegions(any());

    // Mock response for region rules fetch
    doAnswer(
            invocation -> {
              GetRegionRulesFilter filter =
                  invocation.getArgument(0, GetAllRegionRulesRequest.class).getFilter();
              List<RegionRule> regionRules =
                  List.of(expiredRegionRule, alwaysActiveRegionRule, activeRegionRule).stream()
                      .filter(
                          rule ->
                              !rule.hasRuleScope()
                                  || rule.getRuleScope()
                                      .getEnvironmentScope()
                                      .equals(filter.getRuleScope().getEnvironmentScope()))
                      .collect(Collectors.toList());
              return GetAllRegionRulesResponse.newBuilder().addAllRule(regionRules).build();
            })
        .when(regionConfigServiceStub)
        .getAllRegionRules(any());
  }

  @Test
  void testFetchRegionRules() {
    assertEquals(
        List.of(detailedRegion2, detailedRegion3),
        regionRulesFetcher.fetchRegionRules(REQUEST_CONTEXT, Optional.of(environmentId)));
    assertEquals(
        List.of(detailedRegion2),
        regionRulesFetcher.fetchRegionRules(REQUEST_CONTEXT, Optional.empty()));
  }

  private static final RegionRule expiredRegionRule =
      RegionRule.newBuilder()
          .setName("rule-1")
          .setId("Expired-region-rule")
          .setExpirationDetails(
              RegionRule.ExpirationDetails.newBuilder()
                  .setDuration("P1D")
                  .setTimestampMillis(TIMESTAMP + Duration.parse("-P2D").toMillis()))
          .addRegionId("region-id-1")
          .setRuleScope(
              RuleScope.newBuilder()
                  .setEnvironmentScope(
                      EnvironmentScope.newBuilder().addEnvironmentIds(environmentId)))
          .build();

  private static final RegionRule alwaysActiveRegionRule =
      RegionRule.newBuilder()
          .setName("rule-2")
          .setId("Always-Active-region-rule")
          .addRegionId("region-id-2")
          .build();

  private static final RegionRule activeRegionRule =
      RegionRule.newBuilder()
          .setName("rule-3")
          .setId("Active-region-rule")
          .setExpirationDetails(
              RegionRule.ExpirationDetails.newBuilder()
                  .setDuration("P1D")
                  .setTimestampMillis(TIMESTAMP + Duration.parse("P1D").toMillis()))
          .addRegionId("region-id-3")
          .setRuleScope(
              RuleScope.newBuilder()
                  .setEnvironmentScope(
                      EnvironmentScope.newBuilder().addEnvironmentIds(environmentId)))
          .build();

  private static final DetailedRegion detailedRegion1 = mock(DetailedRegion.class);
  private static final DetailedRegion detailedRegion2 = mock(DetailedRegion.class);
  private static final DetailedRegion detailedRegion3 = mock(DetailedRegion.class);

  private static final Map<String, DetailedRegion> REGIONS_MAP =
      Map.of(
          "region-id-1",
          detailedRegion1,
          "region-id-2",
          detailedRegion2,
          "region-id-3",
          detailedRegion3);
}
