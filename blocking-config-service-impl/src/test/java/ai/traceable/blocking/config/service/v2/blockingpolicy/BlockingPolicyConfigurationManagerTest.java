package ai.traceable.blocking.config.service.v2.blockingpolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplierContext;
import ai.traceable.blocking.config.service.v2.AgentCapabilities;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.BlockingDetails;
import ai.traceable.blocking.config.service.v2.BlockingPolicyConfiguration;
import ai.traceable.blocking.config.service.v2.BlockingPolicyConfigurationRequest;
import ai.traceable.blocking.config.service.v2.Component;
import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.config.utils.UuidGenerator;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class BlockingPolicyConfigurationManagerTest {

  private final BlockingRulesSupplierContext blockingRulesSupplierContext =
      new BlockingRulesSupplierContext(Collections.emptyMap(), null);

  @Test
  void getBlockingPolicyConfiguration() {
    GenericBlockingDetailsAggregator<BlockingDetails> mockAggregator =
        (GenericBlockingDetailsAggregator<BlockingDetails>)
            Mockito.mock(GenericBlockingDetailsAggregator.class);

    UuidGenerator mockUuidGenerator = mock(UuidGenerator.class);
    BlockingConfigManagerBase manager =
        new BlockingPolicyConfigurationManager(
            mockAggregator, new SemanticVersioningComparator(), mockUuidGenerator);

    RequestContext requestContext = RequestContext.forTenantId("TENANT_ID");
    Optional<String> environmentId = Optional.of("environment");

    BlockingDetails blockingDetails1 = Mockito.mock(BlockingDetails.class);
    BlockingDetails blockingDetails2 = Mockito.mock(BlockingDetails.class);
    List<BlockingDetails> mockBlockingDetailsList = List.of(blockingDetails1, blockingDetails2);

    doReturn(mockBlockingDetailsList)
        .when(mockAggregator)
        .getBlockingDetails(
            requestContext,
            BlockingPolicyDataFilter.builder()
                .serviceNames(List.of("service-1", "service-2"))
                .environmentId(environmentId)
                .minLibtraceableVersion("1.2.3-rc.4")
                .build());
    doReturn("mock-hash")
        .when(mockUuidGenerator)
        .generateId(any(BlockingPolicyConfiguration.class));

    // Test in case hashes don't match the new policy config is loaded
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setBlockingPolicyConfiguration(
                    BlockingPolicyConfiguration.newBuilder()
                        .addAllBlockingDetailsList(mockBlockingDetailsList))
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(Component.newBuilder().setLibtraceableVersion("1.2.3-rc.4"))
                        .addComponents(Component.newBuilder().setServiceName("service-1")))
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(Component.newBuilder().setLibtraceableVersion("1.2.3-rc.5"))
                        .addComponents(Component.newBuilder().setServiceName("service-2")))
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random")
                    .setBlockingPolicyConfigurationRequest(
                        BlockingPolicyConfigurationRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("1.2.3-rc.4"))
                            .addComponents(Component.newBuilder().setServiceName("service-1")))
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setBlockingPolicyConfigurationRequest(
                        BlockingPolicyConfigurationRequest.getDefaultInstance())
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("1.2.3-rc.5"))
                            .addComponents(Component.newBuilder().setServiceName("service-2")))
                    .build()),
            new BlockingRulesSupplier(
                blockingRulesSupplierContext, requestContext, environmentId)));

    // Test in case hashes do match the new policy is empty
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setBlockingPolicyConfiguration(BlockingPolicyConfiguration.getDefaultInstance())
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setBlockingPolicyConfigurationRequest(
                        BlockingPolicyConfigurationRequest.getDefaultInstance())
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("mock-hash")
                    .setBlockingPolicyConfigurationRequest(
                        BlockingPolicyConfigurationRequest.getDefaultInstance())
                    .build(),
                BlockingConfigRequestElement.newBuilder().setPreviousHash("random-hash").build()),
            new BlockingRulesSupplier(
                blockingRulesSupplierContext, requestContext, environmentId)));

    // Test empty in case request elements are empty
    assertEquals(
        List.of(),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder().setPreviousHash("random-hash").build()),
            new BlockingRulesSupplier(
                blockingRulesSupplierContext, requestContext, environmentId)));
  }
}
