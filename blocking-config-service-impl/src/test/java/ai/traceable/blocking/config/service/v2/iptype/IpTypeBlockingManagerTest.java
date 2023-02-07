package ai.traceable.blocking.config.service.v2.iptype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleAggregatorBase;
import ai.traceable.blocking.config.service.v2.IpTypeBlockingRules;
import ai.traceable.blocking.config.service.v2.IpTypeRule;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class IpTypeBlockingManagerTest {

  @Test
  void getEnabledBlockingRules() {
    IpTypeRuleAggregatorBase<IpTypeRule> mockAggregator =
        (IpTypeRuleAggregatorBase<IpTypeRule>) Mockito.mock(IpTypeRuleAggregatorBase.class);

    IpTypeBlockingManager manager = new DefaultIpTypeBlockingManager(mockAggregator);

    RequestContext requestContext = RequestContext.forTenantId("TENANT_ID");
    Optional<String> environmentId = Optional.of("environment");

    IpTypeRule ipTypeRule1 = Mockito.mock(IpTypeRule.class);
    IpTypeRule ipTypeRule2 = Mockito.mock(IpTypeRule.class);
    List<IpTypeRule> mockIpTypeRuleList = List.of(ipTypeRule1, ipTypeRule2);

    doReturn(mockIpTypeRuleList)
        .when(mockAggregator)
        .getEnabledBlockingRules(requestContext, environmentId);

    assertEquals(
        IpTypeBlockingRules.newBuilder().addAllIpTypeRuleList(mockIpTypeRuleList).build(),
        manager.getEnabledBlockingRules(requestContext, environmentId));
  }
}
