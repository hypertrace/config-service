package ai.traceable.blocking.config.service.common.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.regions.GenericRegionRuleAggregator.GenericRegionRuleConverter;
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
import java.util.Optional;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

class GenericRegionRuleAggregatorTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String environmentId = "env-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final Long TIMESTAMP = 10000L;

  private Clock clock;
  private RegionConfigServiceBlockingStub regionConfigServiceStub;
  private GenericRegionRuleConverter<String> mockRuleConverter;
  private GenericRegionRuleAggregator<String> regionRuleAggregator;

  @BeforeEach
  void setup() {
    clock = mock(Clock.class);
    regionConfigServiceStub = mock(RegionConfigServiceBlockingStub.class, Answers.RETURNS_SELF);
    mockRuleConverter = (GenericRegionRuleConverter<String>) mock(GenericRegionRuleConverter.class);
    regionRuleAggregator =
        new GenericRegionRuleAggregator<>(
            clock, regionConfigServiceStub, mockRuleConverter, ClientConfig.DEFAULT);

    when(this.clock.millis()).thenReturn(TIMESTAMP);

    // Mock response for detailed rules fetch
    doReturn(
            GetDetailedRegionsResponse.newBuilder()
                .addRegion(detailedRegion2)
                .addRegion(detailedRegion3)
                .build())
        .when(regionConfigServiceStub)
        .getDetailedRegions(
            GetDetailedRegionsRequest.newBuilder()
                .setFilter(RegionsFilter.newBuilder().addId("region-id-2").addId("region-id-3"))
                .build());

    // Mock output from converter
    doReturn("converted-2").when(mockRuleConverter).convert(detailedRegion2);
    doReturn("converted-3").when(mockRuleConverter).convert(detailedRegion3);
  }

  @Test
  void testGetEnabledRules_withEnvironment() {
    // Mock response for fetch all rules
    doReturn(
            GetAllRegionRulesResponse.newBuilder()
                .addAllRule(List.of(expiredRegionRule, alwaysActiveRegionRule, activeRegionRule))
                .build())
        .when(regionConfigServiceStub)
        .getAllRegionRules(
            GetAllRegionRulesRequest.newBuilder()
                .setFilter(
                    GetRegionRulesFilter.newBuilder()
                        .setDisabled(false)
                        .setRuleScope(
                            RuleScope.newBuilder()
                                .setEnvironmentScope(
                                    EnvironmentScope.newBuilder()
                                        .addEnvironmentIds(environmentId))))
                .build());

    List<String> rules =
        this.regionRuleAggregator.getEnabledBlockingRules(
            REQUEST_CONTEXT, Optional.of(environmentId));
    assertEquals(List.of("converted-2", "converted-3"), rules);
  }

  @Test
  void testGetEnabledRules_withoutEnvironment() {
    // Mock response for fetch all rules
    doReturn(
            GetAllRegionRulesResponse.newBuilder()
                .addAllRule(List.of(expiredRegionRule, alwaysActiveRegionRule, activeRegionRule))
                .build())
        .when(regionConfigServiceStub)
        .getAllRegionRules(
            GetAllRegionRulesRequest.newBuilder()
                .setFilter(
                    GetRegionRulesFilter.newBuilder()
                        .setDisabled(false)
                        .setRuleScope(
                            RuleScope.newBuilder()
                                .setEnvironmentScope(EnvironmentScope.getDefaultInstance())))
                .build());

    List<String> rules =
        this.regionRuleAggregator.getEnabledBlockingRules(REQUEST_CONTEXT, Optional.empty());
    assertEquals(List.of("converted-2", "converted-3"), rules);
  }

  private final RegionRule expiredRegionRule =
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

  private final RegionRule alwaysActiveRegionRule =
      RegionRule.newBuilder()
          .setName("rule-2")
          .setId("Always-Active-region-rule")
          .addRegionId("region-id-2")
          .setRuleScope(
              RuleScope.newBuilder()
                  .setEnvironmentScope(
                      EnvironmentScope.newBuilder().addEnvironmentIds(environmentId)))
          .build();

  private final RegionRule activeRegionRule =
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

  private static final DetailedRegion detailedRegion2 = mock(DetailedRegion.class);

  private static final DetailedRegion detailedRegion3 = mock(DetailedRegion.class);
}
