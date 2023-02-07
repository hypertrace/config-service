package ai.traceable.blocking.config.service.v2.regions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;

import ai.traceable.blocking.config.service.common.regions.GenericRegionRuleAggregator;
import ai.traceable.blocking.config.service.v2.RegionBlockingRules;
import ai.traceable.blocking.config.service.v2.RegionIpBlockingRule;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class RegionBlockingManagerTest {
  @Test
  void getEnabledBlockingRules() {
    GenericRegionRuleAggregator<RegionIpBlockingRule> mockAggregator =
        (GenericRegionRuleAggregator<RegionIpBlockingRule>)
            Mockito.mock(GenericRegionRuleAggregator.class);

    RegionBlockingManager manager = new DefaultRegionBlockingManager(mockAggregator);

    RequestContext requestContext = RequestContext.forTenantId("TENANT_ID");
    Optional<String> environmentId = Optional.of("environment");

    RegionIpBlockingRule regionIpBlockingRule1 = Mockito.mock(RegionIpBlockingRule.class);
    RegionIpBlockingRule regionIpBlockingRule2 = Mockito.mock(RegionIpBlockingRule.class);
    List<RegionIpBlockingRule> mockRegionIpBlockingRules =
        List.of(regionIpBlockingRule1, regionIpBlockingRule2);

    doReturn(mockRegionIpBlockingRules)
        .when(mockAggregator)
        .getEnabledBlockingRules(requestContext, environmentId);

    assertEquals(
        RegionBlockingRules.newBuilder()
            .addAllRegionIpBlockingRules(mockRegionIpBlockingRules)
            .build(),
        manager.getEnabledBlockingRules(requestContext, environmentId));
  }
}
