package ai.traceable.blocking.config.service.v2.iptype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.v2.AgentCapabilities;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.BlockingPolicyConfigurationRequest;
import ai.traceable.blocking.config.service.v2.Component;
import ai.traceable.blocking.config.service.v2.IpTypeBlockingRules;
import ai.traceable.blocking.config.service.v2.IpTypeBlockingRulesRequest;
import ai.traceable.blocking.config.service.v2.IpTypeRule;
import ai.traceable.config.utils.UuidGenerator;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class IpTypeBlockingManagerTest {

  @Test
  void getEnabledBlockingRules() {
    UuidGenerator mockUuidGenerator = mock(UuidGenerator.class);
    IpTypeRuleConverter ipTypeRuleConverter = new IpTypeRuleConverter();
    BlockingConfigManagerBase manager =
        new IpTypeBlockingManager(ipTypeRuleConverter, mockUuidGenerator);

    RequestContext requestContext = RequestContext.forTenantId("TENANT_ID");
    Optional<String> environmentId = Optional.of("environment");

    IpTypeRule ipTypeRule1 = Mockito.mock(IpTypeRule.class);
    IpTypeRule ipTypeRule2 = Mockito.mock(IpTypeRule.class);
    IpTypeRule ipTypeRule3 = Mockito.mock(IpTypeRule.class);
    List<IpTypeRule> mockIpTypeRuleList = List.of(ipTypeRule1, ipTypeRule2);
    List<IpTypeRule> mockServiceIpTypeRuleList = List.of(ipTypeRule2, ipTypeRule3);

    AgentCapabilities agentCapabilities1 = Mockito.mock(AgentCapabilities.class);
    AgentCapabilities agentCapabilities2 = Mockito.mock(AgentCapabilities.class);
    AgentCapabilities agentCapabilities3 = Mockito.mock(AgentCapabilities.class);
    doReturn(Collections.emptyList()).when(agentCapabilities1).getComponentsList();
    doReturn(Collections.emptyList()).when(agentCapabilities2).getComponentsList();
    doReturn(List.of(Component.newBuilder().setServiceName("serviceName").build()))
        .when(agentCapabilities3)
        .getComponentsList();

    BlockingRulesSupplier blockingRulesSupplier = mock(BlockingRulesSupplier.class);
    doReturn(mockIpTypeRuleList)
        .when(blockingRulesSupplier)
        .getIpTypeIpMappings(any(), eq(Collections.emptySet()));
    doReturn(mockServiceIpTypeRuleList)
        .when(blockingRulesSupplier)
        .getIpTypeIpMappings(any(), eq(Collections.singleton("serviceName")));

    doReturn("mock-hash").when(mockUuidGenerator).generateId(mockIpTypeRuleList);
    doReturn("mock-hash").when(mockUuidGenerator).generateId(mockServiceIpTypeRuleList);

    // Test in case hashes don't match the new ip-type config is loaded
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setIpTypeBlockingRules(
                    IpTypeBlockingRules.newBuilder().addAllIpTypeRuleList(mockIpTypeRuleList))
                .addAgentCapabilities(agentCapabilities1)
                .addAgentCapabilities(agentCapabilities2)
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities1)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities2)
                    .build()),
            blockingRulesSupplier));

    // Test in case hashes do match the new ip-type config is empty
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setIpTypeBlockingRules(IpTypeBlockingRules.getDefaultInstance())
                .addAgentCapabilities(agentCapabilities1)
                .addAgentCapabilities(agentCapabilities2)
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities1)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities2)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random-hash")
                    .setBlockingPolicyConfigurationRequest(
                        BlockingPolicyConfigurationRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities1)
                    .build()),
            blockingRulesSupplier));

    // Test in case hashes do match the new ip-type config for all but one with the service-name
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setIpTypeBlockingRules(
                    IpTypeBlockingRules.newBuilder().addAllIpTypeRuleList(mockIpTypeRuleList))
                .addAgentCapabilities(agentCapabilities1)
                .addAgentCapabilities(agentCapabilities2)
                .addAgentCapabilities(agentCapabilities3)
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities1)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities2)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random-hash")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities3)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random-hash")
                    .setBlockingPolicyConfigurationRequest(
                        BlockingPolicyConfigurationRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities3)
                    .build()),
            blockingRulesSupplier));

    // Test in case hashes do match the new ip-type config and different hash for the
    // service-component
    doReturn("mock-hash2").when(mockUuidGenerator).generateId(mockServiceIpTypeRuleList);
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setIpTypeBlockingRules(IpTypeBlockingRules.getDefaultInstance())
                .addAgentCapabilities(agentCapabilities1)
                .addAgentCapabilities(agentCapabilities2)
                .build(),
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash2")
                .setIpTypeBlockingRules(
                    IpTypeBlockingRules.newBuilder()
                        .addAllIpTypeRuleList(mockServiceIpTypeRuleList))
                .addAgentCapabilities(agentCapabilities3)
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities1)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities2)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random-hash")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities3)
                    .build()),
            blockingRulesSupplier));

    // Test in case hashes don't match the new ip-type config with different hash for
    // service-component
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setIpTypeBlockingRules(
                    IpTypeBlockingRules.newBuilder().addAllIpTypeRuleList(mockIpTypeRuleList))
                .addAgentCapabilities(agentCapabilities1)
                .addAgentCapabilities(agentCapabilities2)
                .build(),
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash2")
                .setIpTypeBlockingRules(
                    IpTypeBlockingRules.newBuilder()
                        .addAllIpTypeRuleList(mockServiceIpTypeRuleList))
                .addAgentCapabilities(agentCapabilities3)
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random-hash")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities1)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities2)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random-hash")
                    .setIpTypeBlockingRulesRequest(IpTypeBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities3)
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
