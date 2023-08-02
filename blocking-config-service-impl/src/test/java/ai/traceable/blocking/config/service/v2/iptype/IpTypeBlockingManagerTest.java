package ai.traceable.blocking.config.service.v2.iptype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.iptype.IpTypeRuleAggregatorBase;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.BlockingPolicyConfigurationRequest;
import ai.traceable.blocking.config.service.v2.IpTypeBlockingRules;
import ai.traceable.blocking.config.service.v2.IpTypeBlockingRulesRequest;
import ai.traceable.blocking.config.service.v2.IpTypeRule;
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

    UuidGenerator mockUuidGenerator = mock(UuidGenerator.class);
    IpTypeRuleConverter ipTypeRuleConverter = new IpTypeRuleConverter();
    BlockingConfigManagerBase manager =
        new IpTypeBlockingManager(ipTypeRuleConverter, mockUuidGenerator);

    RequestContext requestContext = RequestContext.forTenantId("TENANT_ID");
    Optional<String> environmentId = Optional.of("environment");

    IpTypeRule ipTypeRule1 = Mockito.mock(IpTypeRule.class);
    IpTypeRule ipTypeRule2 = Mockito.mock(IpTypeRule.class);
    List<IpTypeRule> mockIpTypeRuleList = List.of(ipTypeRule1, ipTypeRule2);

    doReturn(mockIpTypeRuleList)
        .when(mockAggregator)
        .getEnabledBlockingRules(requestContext, environmentId);

    BlockingRulesSupplier blockingRulesSupplier = mock(BlockingRulesSupplier.class);
    doReturn(mockIpTypeRuleList).when(blockingRulesSupplier).getIpTypeIpMappings(any());

    doReturn("mock-hash").when(mockUuidGenerator).generateId(any(IpTypeBlockingRules.class));

    // Test in case hashes don't match the new ip-type config is loaded
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setIpTypeBlockingRules(
                    IpTypeBlockingRules.newBuilder().addAllIpTypeRuleList(mockIpTypeRuleList))
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .build()),
            blockingRulesSupplier));

    // Test in case hashes do match the new ip-type config is empty
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setIpTypeBlockingRules(IpTypeBlockingRules.getDefaultInstance())
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random-hash")
                    .setBlockingPolicyConfigurationRequest(
                        BlockingPolicyConfigurationRequest.getDefaultInstance())
                    .build()),
            blockingRulesSupplier));

    // Test empty in case request elements are empty
    assertEquals(
        List.of(),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder().setPreviousHash("random-hash").build()),
            blockingRulesSupplier));
  }
}
