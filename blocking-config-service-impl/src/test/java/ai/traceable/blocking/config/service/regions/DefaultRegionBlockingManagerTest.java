package ai.traceable.blocking.config.service.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.v1.RegionBlockingRules;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.EnvironmentScope;
import ai.traceable.region.config.service.v1.ExpirationDetails;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.GetRegionRulesFilter;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import ai.traceable.region.config.service.v1.RuleScope;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultRegionBlockingManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String environmentId = "env-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private final UuidGenerator uuidGenerator = new UuidGenerator();
  private Clock clock;
  private RegionConfigServiceBlockingStub regionConfigServiceStub;
  private MockRegionConfigService mockRegionConfigService;
  private RegionBlockingRulesConverter ruleConverter;

  private DefaultRegionBlockingManager regionBlockingManager;

  @BeforeEach
  void setup() {
    this.clock = mock(Clock.class);
    this.mockRegionConfigService = new MockRegionConfigService();
    this.mockRegionConfigService.start();
    this.regionConfigServiceStub =
        RegionConfigServiceGrpc.newBlockingStub(this.mockRegionConfigService.channel());

    this.ruleConverter =
        new RegionBlockingRulesConverter(regionConfigServiceStub, new IpRangesConverter());
    this.regionBlockingManager =
        new DefaultRegionBlockingManager(
            this.clock, this.regionConfigServiceStub, this.ruleConverter, uuidGenerator);
  }

  @AfterEach
  void teardown() {
    this.mockRegionConfigService.shutdown();
  }

  @Test
  void shouldGetRegionBlockingRules() {
    when(this.clock.millis()).thenReturn(System.currentTimeMillis());
    CreateRegionRuleRequest expiredRegionRuleRequest =
        CreateRegionRuleRequest.newBuilder()
            .setName("rule-1")
            .setExpirationDetails(ExpirationDetails.newBuilder().setDuration("-P1D").build())
            .addRegionId("region-id-1")
            .setRuleScope(
                RuleScope.newBuilder()
                    .setEnvironmentScope(
                        EnvironmentScope.newBuilder().addEnvironmentIds(environmentId).build())
                    .build())
            .build();
    CreateRegionRuleRequest alwaysActiveRegionRuleRequest =
        CreateRegionRuleRequest.newBuilder()
            .setName("rule-2")
            .addRegionId("region-id-2")
            .setRuleScope(
                RuleScope.newBuilder()
                    .setEnvironmentScope(
                        EnvironmentScope.newBuilder()
                            .addEnvironmentIds(environmentId + "random")
                            .build())
                    .build())
            .build();
    CreateRegionRuleRequest activeRegionRuleRequest =
        CreateRegionRuleRequest.newBuilder()
            .setName("rule-3")
            .addRegionId("region-id-3")
            .setExpirationDetails(ExpirationDetails.newBuilder().setDuration("P1D").build())
            .setRuleScope(
                RuleScope.newBuilder()
                    .setEnvironmentScope(
                        EnvironmentScope.newBuilder().addEnvironmentIds(environmentId).build())
                    .build())
            .build();
    CreateRegionRuleRequest disabledRegionRuleRequest =
        CreateRegionRuleRequest.newBuilder()
            .setName("rule-4")
            .addRegionId("region-id-4")
            .setExpirationDetails(ExpirationDetails.newBuilder().setDuration("P1D").build())
            .setRuleScope(
                RuleScope.newBuilder()
                    .setEnvironmentScope(
                        EnvironmentScope.newBuilder().addEnvironmentIds(environmentId).build())
                    .build())
            .build();

    this.regionConfigServiceStub.createRegionRule(expiredRegionRuleRequest);
    this.regionConfigServiceStub.createRegionRule(alwaysActiveRegionRuleRequest);
    this.regionConfigServiceStub.createRegionRule(activeRegionRuleRequest);

    {
      List<RegionRule> allRules =
          this.regionConfigServiceStub
              .getAllRegionRules(
                  GetAllRegionRulesRequest.newBuilder()
                      .setFilter(GetRegionRulesFilter.newBuilder().setDisabled(false))
                      .build())
              .getRuleList();

      // Without environment filter
      List<RegionRule> activeRulesWithoutEnvironmentFilter =
          List.of(allRules.get(0), allRules.get(1));
      RegionBlockingRules regionBlockingRules =
          RegionBlockingRules.newBuilder()
              .addAllRegionIpBlockingRules(
                  ruleConverter.convert(activeRulesWithoutEnvironmentFilter))
              .build();

      RegionBlockingRules responseRules =
          this.regionBlockingManager.getEnabledBlockingRules(REQUEST_CONTEXT, "", Optional.empty());

      assertEquals(
          regionBlockingRules.getRegionIpBlockingRulesList(),
          responseRules.getRegionIpBlockingRulesList());
      assertFalse(responseRules.getHash().isEmpty());
      assertEquals(
          0,
          this.regionBlockingManager
              .getEnabledBlockingRules(REQUEST_CONTEXT, responseRules.getHash(), Optional.empty())
              .getRegionIpBlockingRulesCount());
    }
    {
      // With environment filter
      List<RegionRule> allRules =
          this.regionConfigServiceStub
              .getAllRegionRules(
                  GetAllRegionRulesRequest.newBuilder()
                      .setFilter(
                          GetRegionRulesFilter.newBuilder()
                              .setRuleScope(
                                  RuleScope.newBuilder()
                                      .setEnvironmentScope(
                                          EnvironmentScope.newBuilder()
                                              .addEnvironmentIds(environmentId)
                                              .build())
                                      .build())
                              .setDisabled(false)
                              .build())
                      .build())
              .getRuleList();

      List<RegionRule> activeRulesWithEnvironmentFilter = List.of(allRules.get(0));

      RegionBlockingRules regionBlockingRules =
          RegionBlockingRules.newBuilder()
              .addAllRegionIpBlockingRules(ruleConverter.convert(activeRulesWithEnvironmentFilter))
              .build();

      RegionBlockingRules responseRules =
          this.regionBlockingManager.getEnabledBlockingRules(
              REQUEST_CONTEXT, "", Optional.of(environmentId));

      assertEquals(
          regionBlockingRules.getRegionIpBlockingRulesList(),
          responseRules.getRegionIpBlockingRulesList());
      assertFalse(responseRules.getHash().isEmpty());
      assertEquals(
          0,
          this.regionBlockingManager
              .getEnabledBlockingRules(
                  REQUEST_CONTEXT, responseRules.getHash(), Optional.of(environmentId))
              .getRegionIpBlockingRulesCount());
    }
  }
}
