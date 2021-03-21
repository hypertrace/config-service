package ai.traceable.blocking.config.service.regions;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.v1.BlockingRule;
import ai.traceable.region.config.service.v1.CreateRegionRuleRequest;
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
  private Clock clock;
  private RegionConfigServiceBlockingStub regionConfigServiceStub;
  private MockRegionConfigService mockRegionConfigService;
  private RegionRuleConverter ruleConverter;

  private DefaultRegionBlockingManager regionBlockingManager;

  @BeforeEach
  void setup() {
    this.clock = mock(Clock.class);
    this.mockRegionConfigService = new MockRegionConfigService();
    this.mockRegionConfigService.start();
    this.regionConfigServiceStub =
        RegionConfigServiceGrpc.newBlockingStub(this.mockRegionConfigService.channel());

    this.ruleConverter = mock(RegionRuleConverter.class);
    this.regionBlockingManager =
        new DefaultRegionBlockingManager(
            this.clock, this.regionConfigServiceStub, this.ruleConverter);
  }

  @AfterEach
  void teardown() {
    this.mockRegionConfigService.shutdown();
  }

  @Test
  public void shouldGetRegionBlockingRules() {
    long currentTimeMillis = 123L;
    when(this.clock.millis()).thenReturn(currentTimeMillis);

    CreateRegionRuleRequest expiredRegionRuleRequest =
        CreateRegionRuleRequest.newBuilder()
            .setName("rule-1")
            .setExpirationMillis(currentTimeMillis - 1)
            .build();
    CreateRegionRuleRequest alwaysActiveRegionRuleRequest =
        CreateRegionRuleRequest.newBuilder().setName("rule-2").setExpirationMillis(0).build();
    CreateRegionRuleRequest activeRegionRuleRequest =
        CreateRegionRuleRequest.newBuilder()
            .setName("rule-3")
            .setExpirationMillis(currentTimeMillis + 1)
            .build();

    this.regionConfigServiceStub.createRegionRule(expiredRegionRuleRequest);
    this.regionConfigServiceStub.createRegionRule(alwaysActiveRegionRuleRequest);
    this.regionConfigServiceStub.createRegionRule(activeRegionRuleRequest);

    List<RegionRule> allRules =
        this.regionConfigServiceStub
            .getAllRegionRules(GetAllRegionRulesRequest.getDefaultInstance())
            .getRuleList();
    List<RegionRule> activeRules = List.of(allRules.get(0), allRules.get(1));
    List<BlockingRule> blockingRules = List.of(BlockingRule.getDefaultInstance());
    when(this.ruleConverter.convert(activeRules)).thenReturn(blockingRules);

    assertEquals(blockingRules, this.regionBlockingManager.getBlockingRules());
  }
}
