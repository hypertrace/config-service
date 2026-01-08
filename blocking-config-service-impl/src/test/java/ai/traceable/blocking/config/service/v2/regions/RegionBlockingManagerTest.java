package ai.traceable.blocking.config.service.v2.regions;

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
import ai.traceable.blocking.config.service.v2.Component;
import ai.traceable.blocking.config.service.v2.RegionBlockingRules;
import ai.traceable.blocking.config.service.v2.RegionBlockingRulesRequest;
import ai.traceable.blocking.config.service.v2.RegionIpBlockingRule;
import ai.traceable.config.utils.UuidGenerator;
import java.util.Collections;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class RegionBlockingManagerTest {
  private static final RequestContext requestContext = RequestContext.forTenantId("test-tenant");

  @Test
  void getEnabledBlockingRules() {
    UuidGenerator mockUuidGenerator = mock(UuidGenerator.class);
    RegionIpRulesConverter regionIpRulesConverter =
        new RegionIpRulesConverter(new IpRangesConverter());

    BlockingConfigManagerBase manager =
        new RegionBlockingManager(regionIpRulesConverter, mockUuidGenerator);

    RegionIpBlockingRule regionIpBlockingRule1 = mock(RegionIpBlockingRule.class);
    RegionIpBlockingRule regionIpBlockingRule2 = mock(RegionIpBlockingRule.class);
    RegionIpBlockingRule regionIpBlockingRule3 = mock(RegionIpBlockingRule.class);
    List<RegionIpBlockingRule> mockRegionIpBlockingRules =
        List.of(regionIpBlockingRule1, regionIpBlockingRule2);
    List<RegionIpBlockingRule> mockServiceRegionIpBlockingRules =
        List.of(regionIpBlockingRule1, regionIpBlockingRule2, regionIpBlockingRule3);

    AgentCapabilities agentCapabilities1 = Mockito.mock(AgentCapabilities.class);
    AgentCapabilities agentCapabilities2 = Mockito.mock(AgentCapabilities.class);
    AgentCapabilities agentCapabilities3 = Mockito.mock(AgentCapabilities.class);
    doReturn(Collections.emptyList()).when(agentCapabilities1).getComponentsList();
    doReturn(Collections.emptyList()).when(agentCapabilities2).getComponentsList();
    doReturn(List.of(Component.newBuilder().setServiceName("serviceName").build()))
        .when(agentCapabilities3)
        .getComponentsList();

    BlockingRulesSupplier blockingRulesSupplier = mock(BlockingRulesSupplier.class);
    doReturn(mockRegionIpBlockingRules)
        .when(blockingRulesSupplier)
        .getRegionIpMappings(any(), eq(Collections.emptySet()));
    doReturn(mockServiceRegionIpBlockingRules)
        .when(blockingRulesSupplier)
        .getRegionIpMappings(any(), eq(Collections.singleton("serviceName")));

    doReturn("mock-hash").when(mockUuidGenerator).generateId(mockRegionIpBlockingRules);
    doReturn("mock-hash").when(mockUuidGenerator).generateId(mockServiceRegionIpBlockingRules);

    // Test in case hashes don't match the new region config is loaded
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setRegionBlockingRules(
                    RegionBlockingRules.newBuilder()
                        .addAllRegionIpBlockingRules(mockRegionIpBlockingRules))
                .addAgentCapabilities(agentCapabilities1)
                .addAgentCapabilities(agentCapabilities2)
                .build()),
        manager.generateBlockingElements(
            requestContext,
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities1)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities2)
                    .build()),
            blockingRulesSupplier));

    // Test in case hashes do match the new region config is empty
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setRegionBlockingRules(RegionBlockingRules.getDefaultInstance())
                .addAgentCapabilities(agentCapabilities1)
                .addAgentCapabilities(agentCapabilities2)
                .build()),
        manager.generateBlockingElements(
            requestContext,
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities1)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities2)
                    .build(),
                BlockingConfigRequestElement.newBuilder().setPreviousHash("random-hash").build()),
            blockingRulesSupplier));

    // Test in case hashes do match the new region config for all but one with the service-name
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setRegionBlockingRules(
                    RegionBlockingRules.newBuilder()
                        .addAllRegionIpBlockingRules(mockRegionIpBlockingRules))
                .addAgentCapabilities(agentCapabilities1)
                .addAgentCapabilities(agentCapabilities2)
                .addAgentCapabilities(agentCapabilities3)
                .build()),
        manager.generateBlockingElements(
            requestContext,
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities1)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities2)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random-hash")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities3)
                    .build(),
                BlockingConfigRequestElement.newBuilder().setPreviousHash("random-hash").build()),
            blockingRulesSupplier));

    // Test in case hashes do match the new region config and different hash for the
    // service-component
    doReturn("mock-hash2").when(mockUuidGenerator).generateId(mockServiceRegionIpBlockingRules);
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setRegionBlockingRules(RegionBlockingRules.getDefaultInstance())
                .addAgentCapabilities(agentCapabilities1)
                .addAgentCapabilities(agentCapabilities2)
                .build(),
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash2")
                .setRegionBlockingRules(
                    RegionBlockingRules.newBuilder()
                        .addAllRegionIpBlockingRules(mockServiceRegionIpBlockingRules))
                .addAgentCapabilities(agentCapabilities3)
                .build()),
        manager.generateBlockingElements(
            requestContext,
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities1)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities2)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random-hash")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities3)
                    .build(),
                BlockingConfigRequestElement.newBuilder().setPreviousHash("random-hash").build()),
            blockingRulesSupplier));

    // Test in case hashes don't match the new region config with different hash for
    // service-component
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setRegionBlockingRules(
                    RegionBlockingRules.newBuilder()
                        .addAllRegionIpBlockingRules(mockRegionIpBlockingRules))
                .addAgentCapabilities(agentCapabilities1)
                .addAgentCapabilities(agentCapabilities2)
                .build(),
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash2")
                .setRegionBlockingRules(
                    RegionBlockingRules.newBuilder()
                        .addAllRegionIpBlockingRules(mockServiceRegionIpBlockingRules))
                .addAgentCapabilities(agentCapabilities3)
                .build()),
        manager.generateBlockingElements(
            requestContext,
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random-hash")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities1)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities2)
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random-hash")
                    .setRegionBlockingRulesRequest(RegionBlockingRulesRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(agentCapabilities3)
                    .build(),
                BlockingConfigRequestElement.newBuilder().setPreviousHash("random-hash").build()),
            blockingRulesSupplier));

    // Test empty in case request elements are empty
    assertEquals(
        List.of(),
        manager.generateBlockingElements(
            requestContext,
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random-hash")
                    .addSupportedAgentCapabilities(agentCapabilities1)
                    .build()),
            blockingRulesSupplier));
  }
}
