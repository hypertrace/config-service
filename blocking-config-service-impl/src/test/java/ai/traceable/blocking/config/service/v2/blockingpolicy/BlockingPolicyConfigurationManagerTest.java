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
import ai.traceable.blocking.config.service.v2.IpResolutionStrategy;
import ai.traceable.blocking.config.service.v2.blockingpolicy.exclusion.ExclusionRuleConverter;
import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionModsecRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint;
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
            requestContext,
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
            requestContext,
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
            requestContext,
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
            requestContext,
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
  void testIpResolutionStrategyListsIncludedInResponse() {
    BlockingConfigManagerBase manager =
        new BlockingPolicyConfigurationManager(
            mockBlockingDetailsAggregator,
            mockExclusionRuleConverter,
            new SemanticVersioningComparator(),
            mockUuidGenerator);

    BlockingDetails blockingDetails1 = Mockito.mock(BlockingDetails.class);
    List<BlockingDetails> mockBlockingDetailsList = List.of(blockingDetails1);

    doReturn(Map.of()).when(mockBlockingRulesSupplier).getExclusionRules(any());
    doReturn(new BlockingPolicyAggregate<>(mockBlockingDetailsList))
        .when(mockBlockingDetailsAggregator)
        .getBlockingDetails(any(), any(), any());

    IpResolutionStrategy strategy1 =
        IpResolutionStrategy.newBuilder().setEvaluateAllIpsForBlocking(true).build();
    IpResolutionStrategy strategy2 =
        IpResolutionStrategy.newBuilder().setEvaluateAllIpsForBlocking(false).build();
    doReturn(Map.of("service-1", List.of(strategy1, strategy2)))
        .when(mockBlockingRulesSupplier)
        .getIpResolutionStrategyLists(any());

    doReturn("hash-with-ip-strategy-list")
        .when(mockUuidGenerator)
        .generateId(
            BlockingPolicyConfiguration.newBuilder()
                .addAllBlockingDetailsList(mockBlockingDetailsList)
                .addAllIpResolutionStrategyList(List.of(strategy1, strategy2))
                .build());

    List<BlockingConfigResponseElement> result =
        manager.generateBlockingElements(
            requestContext,
            List.of(buildRequestElement("random", "1.2.3-rc.4", "service-1")),
            mockBlockingRulesSupplier);

    assertEquals(1, result.size());
    assertEquals("hash-with-ip-strategy-list", result.get(0).getHash());
    assertEquals(
        List.of(strategy1, strategy2),
        result.get(0).getBlockingPolicyConfiguration().getIpResolutionStrategyListList());
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
        buildInlineExclusionModsecRule("rule-1");
    DetectionExclusionModsecRule detectionExclusionModsecRule2 =
        buildInlineExclusionModsecRule("rule-2");
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
            requestContext,
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
  void testExclusionRulesServiceScoping() {
    BlockingDetails blockingDetails1 = mock(BlockingDetails.class);
    BlockingDetails blockingDetails2 = mock(BlockingDetails.class);
    List<BlockingDetails> blockingDetailsList = List.of(blockingDetails1, blockingDetails2);

    when(mockBlockingDetailsAggregator.getBlockingDetails(any(), any(), any()))
        .thenReturn(new BlockingPolicyAggregate<>(blockingDetailsList));

    DetectionExclusionModsecRule detectionExclusionModsecRule1 =
        buildInlineExclusionModsecRule("rule-1");
    DetectionExclusionModsecRule detectionExclusionModsecRule2 =
        buildInlineExclusionModsecRule("rule-2");
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
                .addAllBlockingDetailsList(blockingDetailsList)
                .addExclusionRules(exclusionRule1)
                .addExclusionRules(exclusionRule2)
                .build());

    doReturn("hash-s2")
        .when(mockUuidGenerator)
        .generateId(
            BlockingPolicyConfiguration.newBuilder()
                .addAllBlockingDetailsList(blockingDetailsList)
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
            requestContext,
            Collections.singletonList(
                buildRequestElement("previousHash", "2.0.0", "s1").toBuilder()
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(Component.newBuilder().setServiceName("s2")))
                    .build()),
            mockBlockingRulesSupplier);

    assertEquals(2, responseElements.size());
    BlockingConfigResponseElement responseElement = responseElements.get(0);
    assertEquals("hash-s1", responseElement.getHash());
    assertEquals(
        List.of(blockingDetails1, blockingDetails2),
        responseElement.getBlockingPolicyConfiguration().getBlockingDetailsListList());
    assertEquals(
        ImmutableList.of(exclusionRule1, exclusionRule2),
        responseElement.getBlockingPolicyConfiguration().getExclusionRulesList());

    responseElement = responseElements.get(1);
    assertEquals("hash-s2", responseElement.getHash());
    assertEquals(
        List.of(blockingDetails1, blockingDetails2),
        responseElement.getBlockingPolicyConfiguration().getBlockingDetailsListList());
    assertEquals(
        ImmutableList.of(exclusionRule2),
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
        buildInlineExclusionModsecRule("rule-1");
    DetectionExclusionModsecRule detectionExclusionModsecRule2 =
        buildInlineExclusionModsecRule("rule-2");
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
            requestContext,
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

  @Test
  void testEdgeOnlyExclusionRulesFilteredOutForInlineAgent() {
    BlockingDetails blockingDetails1 = mock(BlockingDetails.class);
    List<BlockingDetails> blockingDetailsList = ImmutableList.of(blockingDetails1);

    when(mockBlockingDetailsAggregator.getBlockingDetails(any(), any(), any()))
        .thenReturn(new BlockingPolicyAggregate<>(blockingDetailsList));

    DetectionExclusionModsecRule inlineRule = buildInlineExclusionModsecRule("inline-rule");
    DetectionExclusionModsecRule edgeOnlyRule =
        buildExclusionModsecRule(
            "edge-only-rule", List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE));
    DetectionExclusionModsecRule platformOnlyRule =
        buildExclusionModsecRule(
            "platform-only-rule", List.of(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM));
    DetectionExclusionModsecRule inlineAndEdgeRule =
        buildExclusionModsecRule(
            "inline-and-edge-rule",
            List.of(
                RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT,
                RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE));

    ExclusionRule convertedInlineRule = mock(ExclusionRule.class);
    ExclusionRule convertedInlineAndEdgeRule = mock(ExclusionRule.class);
    when(mockExclusionRuleConverter.convert(inlineRule)).thenReturn(convertedInlineRule);
    when(mockExclusionRuleConverter.convert(inlineAndEdgeRule))
        .thenReturn(convertedInlineAndEdgeRule);

    Map<String, List<DetectionExclusionModsecRule>> exclusionRules =
        Map.of("s1", List.of(inlineRule, edgeOnlyRule, platformOnlyRule, inlineAndEdgeRule));
    when(mockBlockingRulesSupplier.getExclusionRules(any())).thenReturn(exclusionRules);

    doReturn("hash-filtered")
        .when(mockUuidGenerator)
        .generateId(
            BlockingPolicyConfiguration.newBuilder()
                .addBlockingDetailsList(blockingDetails1)
                .addExclusionRules(convertedInlineRule)
                .addExclusionRules(convertedInlineAndEdgeRule)
                .build());

    BlockingPolicyConfigurationManager blockingPolicyConfigurationManager =
        new BlockingPolicyConfigurationManager(
            mockBlockingDetailsAggregator,
            mockExclusionRuleConverter,
            new SemanticVersioningComparator(),
            mockUuidGenerator);

    // Request with LibtraceableVersion → inline agent caller
    List<BlockingConfigResponseElement> responseElements =
        blockingPolicyConfigurationManager.generateBlockingElements(
            requestContext,
            Collections.singletonList(buildRequestElement("previousHash", "1.0.0", "s1")),
            mockBlockingRulesSupplier);

    assertEquals(1, responseElements.size());
    BlockingConfigResponseElement responseElement = responseElements.get(0);
    // Only inlineRule and inlineAndEdgeRule should be present; edgeOnlyRule and platformOnlyRule
    // should be filtered out
    assertEquals(
        ImmutableList.of(convertedInlineRule, convertedInlineAndEdgeRule),
        responseElement.getBlockingPolicyConfiguration().getExclusionRulesList());
    // edgeOnlyRule and platformOnlyRule converter should never be called
    Mockito.verify(mockExclusionRuleConverter, Mockito.never()).convert(edgeOnlyRule);
    Mockito.verify(mockExclusionRuleConverter, Mockito.never()).convert(platformOnlyRule);
  }

  private static DetectionExclusionModsecRule buildExclusionModsecRule(
      String ruleId, List<RuleEvaluationPoint> evaluationPoints) {
    return DetectionExclusionModsecRule.newBuilder()
        .setRule(
            DetectionExclusionRule.newBuilder()
                .setId(ruleId)
                .setRuleInfo(
                    DetectionExclusionRuleInfo.newBuilder()
                        .addAllRuleEvaluationPoints(evaluationPoints)))
        .build();
  }

  private static DetectionExclusionModsecRule buildInlineExclusionModsecRule(String ruleId) {
    return DetectionExclusionModsecRule.newBuilder()
        .setRule(
            DetectionExclusionRule.newBuilder()
                .setId(ruleId)
                .setRuleInfo(
                    DetectionExclusionRuleInfo.newBuilder()
                        .addRuleEvaluationPoints(
                            RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)))
        .build();
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
