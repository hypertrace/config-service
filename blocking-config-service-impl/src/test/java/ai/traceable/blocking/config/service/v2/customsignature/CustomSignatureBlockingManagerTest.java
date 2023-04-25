package ai.traceable.blocking.config.service.v2.customsignature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.customsignature.CustomSignatureBlobFetcher;
import ai.traceable.blocking.config.service.v2.AgentCapabilities;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.Component;
import ai.traceable.blocking.config.service.v2.CustomSignatureBlockingRules;
import ai.traceable.blocking.config.service.v2.CustomSignatureBlockingRulesRequest;
import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CustomSignatureBlockingManagerTest {
  private static final String V3_blob = "Tester rule blob v3";
  private static final String V3_seg_arg_blob = "Tester rule blob v3 seg arg limit";
  private static final UuidGenerator uuidGenerator = new UuidGenerator();
  private static final RequestContext requestContext = RequestContext.forTenantId("TENANT_ID");
  private static final Optional<String> environmentId = Optional.of("environment");
  private static final String V3_HASH = uuidGenerator.generateId(V3_blob);
  private static final String V3_SEG_ARG_HASH = uuidGenerator.generateId(V3_seg_arg_blob);

  private static BlockingConfigManagerBase manager;

  @BeforeAll
  static void setup() {
    CustomSignatureBlobFetcher mockCustomSignatureBlobFetcher =
        mock(CustomSignatureBlobFetcher.class);
    SemanticVersioningComparator semanticVersioningComparator = new SemanticVersioningComparator();
    manager =
        new CustomSignatureBlockingManager(
            mockCustomSignatureBlobFetcher, uuidGenerator, semanticVersioningComparator);

    doReturn(V3_blob)
        .when(mockCustomSignatureBlobFetcher)
        .getEnabledCustomSignatureRulesBlob(
            requestContext, CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3, environmentId);
    doReturn(V3_seg_arg_blob)
        .when(mockCustomSignatureBlobFetcher)
        .getEnabledCustomSignatureRulesBlob(
            requestContext,
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS,
            environmentId);
  }

  @Test
  void ruleFormationV3Only() {
    // Test backward compatibility for old agents
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash(V3_HASH)
                .setCustomSignatureBlockingRules(
                    CustomSignatureBlockingRules.newBuilder().setCustomSignatureRulesBlob(V3_blob))
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random")
                    .setCustomSignatureBlockingRulesRequest(
                        CustomSignatureBlockingRulesRequest.getDefaultInstance())
                    .build()),
            requestContext,
            environmentId));

    // Old libtraceable version
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash(V3_HASH)
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                .setCustomSignatureBlockingRules(
                    CustomSignatureBlockingRules.newBuilder().setCustomSignatureRulesBlob(V3_blob))
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("hg")
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                    .setCustomSignatureBlockingRulesRequest(
                        CustomSignatureBlockingRulesRequest.getDefaultInstance())
                    .build()),
            requestContext,
            environmentId));

    // Same hash for all
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash(V3_HASH)
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                .setCustomSignatureBlockingRules(CustomSignatureBlockingRules.getDefaultInstance())
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash(V3_HASH)
                    .setCustomSignatureBlockingRulesRequest(
                        CustomSignatureBlockingRulesRequest.getDefaultInstance())
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash(V3_HASH)
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                    .setCustomSignatureBlockingRulesRequest(
                        CustomSignatureBlockingRulesRequest.getDefaultInstance())
                    .build()),
            requestContext,
            environmentId));

    // Same hash for some
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash(V3_HASH)
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                .setCustomSignatureBlockingRules(
                    CustomSignatureBlockingRules.newBuilder().setCustomSignatureRulesBlob(V3_blob))
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random")
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder()
                                    .setTraceablePlatformAgentVersion("0.1.98-rc.148")))
                    .setCustomSignatureBlockingRulesRequest(
                        CustomSignatureBlockingRulesRequest.getDefaultInstance())
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash(V3_HASH)
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                    .setCustomSignatureBlockingRulesRequest(
                        CustomSignatureBlockingRulesRequest.getDefaultInstance())
                    .build()),
            requestContext,
            environmentId));
  }

  @Test
  void ruleFormationMixed() {
    // Different libtraceable version
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash(V3_HASH)
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                .setCustomSignatureBlockingRules(
                    CustomSignatureBlockingRules.newBuilder().setCustomSignatureRulesBlob(V3_blob))
                .build(),
            getV4Response(
                List.of(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder().setLibtraceableVersion("0.1.98-rc.139"))
                        .build(),
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder().setLibtraceableVersion("0.1.98-rc.140"))
                        .build()))),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random")
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.139")))
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.140")))
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                    .setCustomSignatureBlockingRulesRequest(
                        CustomSignatureBlockingRulesRequest.getDefaultInstance())
                    .build()),
            requestContext,
            environmentId));

    // Same hash for all
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash(V3_HASH)
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                .setCustomSignatureBlockingRules(CustomSignatureBlockingRules.getDefaultInstance())
                .build(),
            BlockingConfigResponseElement.newBuilder()
                .setHash(V3_SEG_ARG_HASH)
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder().setLibtraceableVersion("0.1.98-rc.139"))
                        .build())
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder().setLibtraceableVersion("0.1.98-rc.149"))
                        .build())
                .setCustomSignatureBlockingRules(CustomSignatureBlockingRules.getDefaultInstance())
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash(V3_SEG_ARG_HASH)
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.139")))
                    .setCustomSignatureBlockingRulesRequest(
                        CustomSignatureBlockingRulesRequest.getDefaultInstance())
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash(V3_HASH)
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                    .setCustomSignatureBlockingRulesRequest(
                        CustomSignatureBlockingRulesRequest.getDefaultInstance())
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash(V3_SEG_ARG_HASH)
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.149")))
                    .setCustomSignatureBlockingRulesRequest(
                        CustomSignatureBlockingRulesRequest.getDefaultInstance())
                    .build()),
            requestContext,
            environmentId));

    // Test empty in case request elements are empty
    assertEquals(
        List.of(),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash(V3_HASH)
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.148")))
                    .build()),
            requestContext,
            environmentId));
  }

  private static BlockingConfigResponseElement getV4Response(
      List<AgentCapabilities> agentCapabilities) {
    return BlockingConfigResponseElement.newBuilder()
        .setHash(V3_SEG_ARG_HASH)
        .addAllAgentCapabilities(agentCapabilities)
        .setCustomSignatureBlockingRules(
            CustomSignatureBlockingRules.newBuilder().setCustomSignatureRulesBlob(V3_seg_arg_blob))
        .build();
  }
}
