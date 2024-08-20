package ai.traceable.blocking.config.service.v2.blockingpolicy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.blocking.config.service.common.blockingpolicy.GenericBlockingDetailsAggregator;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyAggregate;
import ai.traceable.blocking.config.service.common.blockingpolicy.fetchers.BlockingPolicyDataFetcherBase.BlockingPolicyDataFilter;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.v2.ActorDetails;
import ai.traceable.blocking.config.service.v2.AgentCapabilities;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.BlockingDetails;
import ai.traceable.blocking.config.service.v2.BlockingPolicyConfiguration;
import ai.traceable.blocking.config.service.v2.BlockingPolicyConfigurationRequest;
import ai.traceable.blocking.config.service.v2.Component;
import ai.traceable.blocking.config.service.v2.ExclusionRule;
import ai.traceable.blocking.config.service.v2.blockingpolicy.exclusion.ExclusionRuleConverter;
import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionModsecRule;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

class BlockingPolicyConfigurationManagerTest {
  private static final RequestContext requestContext = RequestContext.forTenantId("TENANT_ID");
  private static final Optional<String> environmentId = Optional.of("environment");

  @Mock private GenericBlockingDetailsAggregator<BlockingDetails> mockBlockingDetailsAggregator;
  @Mock private BlockingRulesSupplier mockBlockingRulesSupplier;
  @Mock private UuidGenerator mockUuidGenerator;
  @Mock private ExclusionRuleConverter mockExclusionRuleConverter;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(mockBlockingRulesSupplier.getRequestContext()).thenReturn(requestContext);
    when(mockBlockingRulesSupplier.getEnvironmentId()).thenReturn(environmentId);
  }

  @Test
  void getBlockingPolicyConfiguration() {
    BlockingConfigManagerBase manager =
        new BlockingPolicyConfigurationManager(
            mockBlockingDetailsAggregator,
            mockExclusionRuleConverter,
            new SemanticVersioningComparator(),
            mockUuidGenerator);

    BlockingDetails blockingDetails1 = Mockito.mock(BlockingDetails.class);
    BlockingDetails blockingDetails2 = Mockito.mock(BlockingDetails.class);
    List<BlockingDetails> mockBlockingDetailsList = List.of(blockingDetails1, blockingDetails2);

    doReturn(Map.of()).when(mockBlockingRulesSupplier).getExclusionRules(any());

    doReturn(new BlockingPolicyAggregate<>(mockBlockingDetailsList))
        .when(mockBlockingDetailsAggregator)
        .getBlockingDetails(
            requestContext,
            BlockingPolicyDataFilter.builder()
                .serviceNames(List.of("service-1", "service-2"))
                .environmentId(environmentId)
                .minLibtraceableVersion("1.2.3-rc.4")
                .build(),
            mockBlockingRulesSupplier);

    doReturn("mock-hash")
        .when(mockUuidGenerator)
        .generateId(
            BlockingPolicyConfiguration.newBuilder()
                .addBlockingDetailsList(blockingDetails1)
                .addBlockingDetailsList(blockingDetails2)
                .build());

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
                buildRequestElement("random", "1.2.3-rc.4", "service-1"),
                buildRequestElement("mock-hash", "1.2.3-rc.5", "service-2")),
            mockBlockingRulesSupplier));

    // Test in case hashes matches the new policy is empty
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("mock-hash")
                .setBlockingPolicyConfiguration(BlockingPolicyConfiguration.getDefaultInstance())
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(Component.newBuilder().setLibtraceableVersion("1.2.3-rc.4"))
                        .addComponents(Component.newBuilder().setServiceName("service-1")))
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(Component.newBuilder().setLibtraceableVersion("1.2.3-rc.4"))
                        .addComponents(Component.newBuilder().setServiceName("service-2")))
                .build()),
        manager.generateBlockingElements(
            List.of(
                buildRequestElement("mock-hash", "1.2.3-rc.4", "service-1").toBuilder()
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("1.2.3-rc.4"))
                            .addComponents(Component.newBuilder().setServiceName("service-2")))
                    .build()),
            mockBlockingRulesSupplier));

    // Test empty in case request elements are empty
    assertEquals(
        List.of(),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder().setPreviousHash("random-hash").build()),
            mockBlockingRulesSupplier));
  }

  @Test
  void testServiceScopedResponse() {
    BlockingConfigManagerBase manager =
        new BlockingPolicyConfigurationManager(
            mockBlockingDetailsAggregator,
            mockExclusionRuleConverter,
            new SemanticVersioningComparator(),
            mockUuidGenerator);

    BlockingDetails blockingDetails1 =
        BlockingDetails.newBuilder()
            .setActorDetails(ActorDetails.newBuilder().setUserId("usr-1"))
            .build();
    BlockingDetails blockingDetails2 =
        BlockingDetails.newBuilder()
            .setActorDetails(ActorDetails.newBuilder().setUserId("usr-2"))
            .build();
    LinkedHashMap<String, List<BlockingDetails>> serviceScopedResponseMap =
        new LinkedHashMap<>(
            Map.of(
                "service-1",
                List.of(blockingDetails1),
                "service-3",
                List.of(blockingDetails1),
                "service-2",
                List.of(blockingDetails2)));

    doReturn(Map.of()).when(mockBlockingRulesSupplier).getExclusionRules(any());

    doReturn(new BlockingPolicyAggregate<>(serviceScopedResponseMap))
        .when(mockBlockingDetailsAggregator)
        .getBlockingDetails(
            requestContext,
            BlockingPolicyDataFilter.builder()
                .serviceNames(List.of("service-1", "service-3", "service-2"))
                .environmentId(environmentId)
                .minLibtraceableVersion("1.2.3-rc.4")
                .build(),
            mockBlockingRulesSupplier);
    doReturn("hash-1")
        .when(mockUuidGenerator)
        .generateId(
            BlockingPolicyConfiguration.newBuilder()
                .addBlockingDetailsList(blockingDetails1)
                .build());

    doReturn("hash-2")
        .when(mockUuidGenerator)
        .generateId(
            BlockingPolicyConfiguration.newBuilder()
                .addBlockingDetailsList(blockingDetails2)
                .build());

    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash("hash-1")
                .setBlockingPolicyConfiguration(
                    BlockingPolicyConfiguration.newBuilder()
                        .addBlockingDetailsList(blockingDetails1))
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(Component.newBuilder().setLibtraceableVersion("1.2.3-rc.4"))
                        .addComponents(Component.newBuilder().setServiceName("service-1")))
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(Component.newBuilder().setLibtraceableVersion("1.2.3-rc.14"))
                        .addComponents(Component.newBuilder().setServiceName("service-3")))
                .build(),
            BlockingConfigResponseElement.newBuilder()
                .setHash("hash-2")
                .setBlockingPolicyConfiguration(
                    BlockingPolicyConfiguration.newBuilder()
                        .addBlockingDetailsList(blockingDetails2))
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(Component.newBuilder().setLibtraceableVersion("1.2.3-rc.5"))
                        .addComponents(Component.newBuilder().setServiceName("service-2")))
                .build()),
        manager.generateBlockingElements(
            List.of(
                buildRequestElement("random", "1.2.3-rc.4", "service-1").toBuilder()
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("1.2.3-rc.14"))
                            .addComponents(Component.newBuilder().setServiceName("service-3")))
                    .build(),
                buildRequestElement("mock-hash", "1.2.3-rc.5", "service-2")),
            mockBlockingRulesSupplier));
  }

  @Test
  void testGenerateBlockingElements_withExclusionRules_linear() {
    BlockingDetails blockingDetails1 = mock(BlockingDetails.class);
    BlockingDetails blockingDetails2 = mock(BlockingDetails.class);

    List<BlockingDetails> blockingDetailsList =
        ImmutableList.of(blockingDetails1, blockingDetails2);

    when(mockBlockingDetailsAggregator.getBlockingDetails(any(), any(), any()))
        .thenReturn(new BlockingPolicyAggregate<>(blockingDetailsList));

    DetectionExclusionModsecRule detectionExclusionModsecRule1 =
        mock(DetectionExclusionModsecRule.class);
    DetectionExclusionModsecRule detectionExclusionModsecRule2 =
        mock(DetectionExclusionModsecRule.class);
    ExclusionRule exclusionRule1 = mock(ExclusionRule.class);
    ExclusionRule exclusionRule2 = mock(ExclusionRule.class);
    when(mockExclusionRuleConverter.convert(detectionExclusionModsecRule1))
        .thenReturn(exclusionRule1);
    when(mockExclusionRuleConverter.convert(detectionExclusionModsecRule2))
        .thenReturn(exclusionRule2);

    Map<String, List<DetectionExclusionModsecRule>> exclusionRules =
        Map.of(
            "s1",
            List.of(detectionExclusionModsecRule1, detectionExclusionModsecRule2),
            "s2",
            List.of(detectionExclusionModsecRule2));
    when(mockBlockingRulesSupplier.getExclusionRules(any())).thenReturn(exclusionRules);

    doReturn("hash-s1")
        .when(mockUuidGenerator)
        .generateId(
            BlockingPolicyConfiguration.newBuilder()
                .addBlockingDetailsList(blockingDetails1)
                .addBlockingDetailsList(blockingDetails2)
                .addExclusionRules(exclusionRule1)
                .addExclusionRules(exclusionRule2)
                .build());

    BlockingPolicyConfigurationManager blockingPolicyConfigurationManager =
        new BlockingPolicyConfigurationManager(
            mockBlockingDetailsAggregator,
            mockExclusionRuleConverter,
            new SemanticVersioningComparator(),
            mockUuidGenerator);
    List<BlockingConfigResponseElement> responseElements =
        blockingPolicyConfigurationManager.generateBlockingElements(
            Collections.singletonList(buildRequestElement("previousHash", "1.0.0", "s1")),
            mockBlockingRulesSupplier);

    assertEquals(1, responseElements.size());
    BlockingConfigResponseElement responseElement = responseElements.get(0);
    assertEquals("hash-s1", responseElement.getHash());
    assertEquals(
        blockingDetailsList,
        responseElement.getBlockingPolicyConfiguration().getBlockingDetailsListList());
    assertEquals(
        ImmutableList.of(exclusionRule1, exclusionRule2),
        responseElement.getBlockingPolicyConfiguration().getExclusionRulesList());
  }

  @Test
  void testGenerateBlockingElements_withExclusionRules_service() {
    BlockingDetails blockingDetails1 = mock(BlockingDetails.class);
    BlockingDetails blockingDetails2 = mock(BlockingDetails.class);

    LinkedHashMap<String, List<BlockingDetails>> serviceBlockingDetails =
        new LinkedHashMap<>(
            Map.of("s1", List.of(blockingDetails1), "s2", List.of(blockingDetails2)));

    when(mockBlockingDetailsAggregator.getBlockingDetails(any(), any(), any()))
        .thenReturn(new BlockingPolicyAggregate<>(serviceBlockingDetails));

    DetectionExclusionModsecRule detectionExclusionModsecRule1 =
        mock(DetectionExclusionModsecRule.class);
    DetectionExclusionModsecRule detectionExclusionModsecRule2 =
        mock(DetectionExclusionModsecRule.class);
    ExclusionRule exclusionRule1 = mock(ExclusionRule.class);
    ExclusionRule exclusionRule2 = mock(ExclusionRule.class);
    when(mockExclusionRuleConverter.convert(detectionExclusionModsecRule1))
        .thenReturn(exclusionRule1);
    when(mockExclusionRuleConverter.convert(detectionExclusionModsecRule2))
        .thenReturn(exclusionRule2);

    Map<String, List<DetectionExclusionModsecRule>> exclusionRules =
        Map.of(
            "s1",
            List.of(detectionExclusionModsecRule1, detectionExclusionModsecRule2),
            "s2",
            List.of(detectionExclusionModsecRule2));
    when(mockBlockingRulesSupplier.getExclusionRules(any())).thenReturn(exclusionRules);

    doReturn("hash-s1")
        .when(mockUuidGenerator)
        .generateId(
            BlockingPolicyConfiguration.newBuilder()
                .addBlockingDetailsList(blockingDetails1)
                .addExclusionRules(exclusionRule1)
                .addExclusionRules(exclusionRule2)
                .build());

    doReturn("hash-s2")
        .when(mockUuidGenerator)
        .generateId(
            BlockingPolicyConfiguration.newBuilder()
                .addBlockingDetailsList(blockingDetails2)
                .addExclusionRules(exclusionRule2)
                .build());

    BlockingPolicyConfigurationManager blockingPolicyConfigurationManager =
        new BlockingPolicyConfigurationManager(
            mockBlockingDetailsAggregator,
            mockExclusionRuleConverter,
            new SemanticVersioningComparator(),
            mockUuidGenerator);

    List<BlockingConfigResponseElement> responseElements =
        blockingPolicyConfigurationManager.generateBlockingElements(
            Collections.singletonList(
                buildRequestElement("previousHash", "1.0.0", "s1").toBuilder()
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(Component.newBuilder().setServiceName("s2")))
                    .build()),
            mockBlockingRulesSupplier);

    assertEquals(2, responseElements.size());
    BlockingConfigResponseElement responseElement = responseElements.get(0);
    assertEquals("hash-s1", responseElement.getHash());
    assertEquals(
        List.of(blockingDetails1),
        responseElement.getBlockingPolicyConfiguration().getBlockingDetailsListList());
    assertEquals(
        ImmutableList.of(exclusionRule1, exclusionRule2),
        responseElement.getBlockingPolicyConfiguration().getExclusionRulesList());

    responseElement = responseElements.get(1);
    assertEquals("hash-s2", responseElement.getHash());
    assertEquals(
        List.of(blockingDetails2),
        responseElement.getBlockingPolicyConfiguration().getBlockingDetailsListList());
    assertEquals(
        ImmutableList.of(exclusionRule2),
        responseElement.getBlockingPolicyConfiguration().getExclusionRulesList());
  }

  private static BlockingConfigRequestElement buildRequestElement(
      String hash, String libtraceableVersion, String serviceName) {
    return BlockingConfigRequestElement.newBuilder()
        .setPreviousHash(hash)
        .setBlockingPolicyConfigurationRequest(
            BlockingPolicyConfigurationRequest.getDefaultInstance())
        .addSupportedAgentCapabilities(
            AgentCapabilities.newBuilder()
                .addComponents(Component.newBuilder().setLibtraceableVersion(libtraceableVersion))
                .addComponents(Component.newBuilder().setServiceName(serviceName)))
        .build();
  }
}
