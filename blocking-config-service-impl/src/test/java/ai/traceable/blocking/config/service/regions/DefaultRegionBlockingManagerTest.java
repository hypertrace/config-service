package ai.traceable.blocking.config.service.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.UuidGenerator;
import ai.traceable.blocking.config.service.v1.RegionBlockingRules;
import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
import ai.traceable.region.config.service.v1.ExpirationDetails;
import ai.traceable.region.config.service.v1.GetAllRegionRulesRequest;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc;
import ai.traceable.region.config.service.v1.RegionConfigServiceGrpc.RegionConfigServiceBlockingStub;
import ai.traceable.region.config.service.v1.RegionRule;
import java.time.Clock;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultRegionBlockingManagerTest {
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
  public void shouldGetRegionBlockingRules() {
    long currentTimeMillis = 1230000000L;
    when(this.clock.millis()).thenReturn(currentTimeMillis);
    CreateRegionRuleRequest expiredRegionRuleRequest =
        CreateRegionRuleRequest.newBuilder()
            .setName("rule-1")
            .setExpirationDetails(ExpirationDetails.newBuilder().setDuration("-P1D").build())
            .build();
    CreateRegionRuleRequest alwaysActiveRegionRuleRequest =
        CreateRegionRuleRequest.newBuilder().setName("rule-2").build();
    CreateRegionRuleRequest activeRegionRuleRequest =
        CreateRegionRuleRequest.newBuilder()
            .setName("rule-3")
            .setExpirationDetails(ExpirationDetails.newBuilder().setDuration("P1D").build())
            .build();

    this.regionConfigServiceStub.createRegionRule(expiredRegionRuleRequest);
    this.regionConfigServiceStub.createRegionRule(alwaysActiveRegionRuleRequest);
    this.regionConfigServiceStub.createRegionRule(activeRegionRuleRequest);

    List<RegionRule> allRules =
        this.regionConfigServiceStub
            .getAllRegionRules(GetAllRegionRulesRequest.getDefaultInstance())
            .getRuleList();
    List<RegionRule> activeRules = List.of(allRules.get(0), allRules.get(1));
    RegionBlockingRules regionBlockingRules =
        RegionBlockingRules.newBuilder()
            .addAllRegionIpBlockingRules(ruleConverter.convert(activeRules))
            .build();

    RegionBlockingRules responseRules = this.regionBlockingManager.getEnabledBlockingRules("");

    assertEquals(
        regionBlockingRules.getRegionIpBlockingRulesList(),
        responseRules.getRegionIpBlockingRulesList());
    assertFalse(responseRules.getHash().isEmpty());
    assertEquals(
        0,
        this.regionBlockingManager
            .getEnabledBlockingRules(responseRules.getHash())
            .getRegionIpBlockingRulesCount());
  }
}
