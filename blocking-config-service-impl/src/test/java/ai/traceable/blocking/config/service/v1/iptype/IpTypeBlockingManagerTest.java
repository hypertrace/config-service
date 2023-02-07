package ai.traceable.blocking.config.service.v1.iptype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleAggregatorBase;
import ai.traceable.blocking.config.service.v1.IpTypeBlockingRules;
import ai.traceable.blocking.config.service.v1.IpTypeRule;
import ai.traceable.config.utils.UuidGenerator;
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
    UuidGenerator uuidGenerator = Mockito.mock(UuidGenerator.class);

    IpTypeBlockingManager manager = new DefaultIpTypeBlockingManager(mockAggregator, uuidGenerator);

    RequestContext requestContext = RequestContext.forTenantId("TENANT_ID");
    Optional<String> environmentId = Optional.of("environment");

    IpTypeRule ipTypeRule1 = Mockito.mock(IpTypeRule.class);
    IpTypeRule ipTypeRule2 = Mockito.mock(IpTypeRule.class);
    List<IpTypeRule> mockIpTypeRuleList = List.of(ipTypeRule1, ipTypeRule2);

    doReturn(mockIpTypeRuleList)
        .when(mockAggregator)
        .getEnabledBlockingRules(requestContext, environmentId);
    doReturn("hash").when(uuidGenerator).generateId(mockIpTypeRuleList);

    assertEquals(
        IpTypeBlockingRules.newBuilder()
            .addAllIpTypeRuleList(mockIpTypeRuleList)
            .setHash("hash")
            .build(),
        manager.getEnabledBlockingRules(requestContext, "", environmentId));

    assertEquals(
        IpTypeBlockingRules.newBuilder().setHash("hash").build(),
        manager.getEnabledBlockingRules(requestContext, "hash", environmentId));
  }
}
