package ai.traceable.blocking.config.service.common.iptype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleAggregatorBase.GenericIpTypeRuleConverter;
import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleInfo.IpRangeInfo;
import ai.traceable.malicioussources.config.service.v1.IpLocationType;
import ai.traceable.malicioussources.config.service.v1.IpLocationTypeCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleCondition;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRuleInfo;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class IpTypeRuleAggregatorBaseTest {
  @Test
  void testGetEnabledRules() {
    IpTypeRulesLoader ipTypeRulesLoader = mock(IpTypeRulesLoader.class);
    BlockingIpTypesClient blockingIpTypesClient = mock(BlockingIpTypesClient.class);

    IpTypeRuleInfo rule1 =
        IpTypeRuleInfo.builder()
            .ipType(IpTypeRuleInfo.IpType.BOT)
            .ipv4Address(11)
            .ipv4Range(new IpRangeInfo(151, 161))
            .build();
    IpTypeRuleInfo rule2 =
        IpTypeRuleInfo.builder()
            .ipType(IpTypeRuleInfo.IpType.ANONYMOUS_VPN)
            .ipv4Address(22)
            .ipv4Range(new IpRangeInfo(252, 262))
            .build();

    Supplier<Map<IpTypeRuleInfo.IpType, IpTypeRuleInfo>> ipTypeRuleSupplier =
        () -> Map.of(IpTypeRuleInfo.IpType.BOT, rule1, IpTypeRuleInfo.IpType.ANONYMOUS_VPN, rule2);
    doReturn(ipTypeRuleSupplier).when(ipTypeRulesLoader).getLatestDataSupplier();

    when(blockingIpTypesClient.fetchMaliciousSourceRules(any(), any()))
        .thenReturn(
            List.of(
                MaliciousSourcesRule.newBuilder()
                    .setRuleInfo(
                        MaliciousSourcesRuleInfo.newBuilder()
                            .addConditions(
                                MaliciousSourcesRuleCondition.newBuilder()
                                    .setIpLocationTypeCondition(
                                        IpLocationTypeCondition.newBuilder()
                                            .addIpLocationTypes(IpLocationType.IP_LOCATION_TYPE_BOT)
                                            .addIpLocationTypes(
                                                IpLocationType.IP_LOCATION_TYPE_HOSTING_PROVIDER))))
                    .build()));

    GenericIpTypeRuleConverter<String> mockIpTypeRuleConverter =
        (GenericIpTypeRuleConverter<String>) Mockito.mock(GenericIpTypeRuleConverter.class);
    // Mock converter response for expected rule
    when(mockIpTypeRuleConverter.convert(rule1)).thenReturn(new ImmutablePair<>("happy", "hash"));

    IpTypeRuleAggregatorBase<String> ipTypeRuleAggregator =
        new IpTypeRuleAggregatorBase<>(
            ipTypeRulesLoader, blockingIpTypesClient, mockIpTypeRuleConverter);

    List<ImmutablePair<String, String>> enabledBlockingRules =
        ipTypeRuleAggregator.getEnabledBlockingRules(
            RequestContext.forTenantId("tenantId"), Optional.empty());

    assertEquals(1, enabledBlockingRules.size());
    assertEquals("happy", enabledBlockingRules.get(0).getLeft());
    assertEquals("hash", enabledBlockingRules.get(0).getRight());
  }
}
