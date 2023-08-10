package ai.traceable.blocking.config.service.v2.customsignature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CustomSignatureBlockingManagerTest {
  private static final String V3_blob = "Tester rule blob v3";
  private static final String V3_seg_arg_blob = "Tester rule blob v3 seg arg limit";
  private static final UuidGenerator uuidGenerator = new UuidGenerator();
  private static final String V3_HASH = uuidGenerator.generateId(V3_blob);
  private static final String V3_SEG_ARG_HASH = uuidGenerator.generateId(V3_seg_arg_blob);

  private static BlockingConfigManagerBase manager;
  private static BlockingRulesSupplier blockingRulesSupplier;

  @BeforeAll
  static void setup() {
    SemanticVersioningComparator semanticVersioningComparator = new SemanticVersioningComparator();
    manager = new CustomSignatureBlockingManager(uuidGenerator, semanticVersioningComparator);
    blockingRulesSupplier = mock(BlockingRulesSupplier.class);

    doReturn(V3_blob)
        .when(blockingRulesSupplier)
        .getCustomSignatureModsecBlob(CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3);
    doReturn(V3_seg_arg_blob)
        .when(blockingRulesSupplier)
        .getCustomSignatureModsecBlob(
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS);
  }

  @Test
  void ruleFormationV3Only() {

    // Incorrect libtraceable version
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash(V3_HASH)
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(Component.newBuilder().setLibtraceableVersion("0.0.2.3")))
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
                                Component.newBuilder().setLibtraceableVersion("0.0.2.3")))
                    .setCustomSignatureBlockingRulesRequest(
                        CustomSignatureBlockingRulesRequest.getDefaultInstance())
                    .build()),
            blockingRulesSupplier));

    // Old libtraceable version
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash(V3_HASH)
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder()
                                .setLibtraceableVersion("0.1.98-rc.110")
                                .setServiceName("serviceName")))
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
                                    .setLibtraceableVersion("0.1.98-rc.110")
                                    .setServiceName("serviceName")))
                    .setCustomSignatureBlockingRulesRequest(
                        CustomSignatureBlockingRulesRequest.getDefaultInstance())
                    .build()),
            blockingRulesSupplier));

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
            blockingRulesSupplier));

    // Same hash for some
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash(V3_HASH)
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder()
                                .setLibtraceableVersion("")
                                .setTraceablePlatformAgentVersion("1.30.2-rc.3")))
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder()
                                .setTraceablePlatformAgentVersion("0.1.98-rc.148")))
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
                                    .setLibtraceableVersion("")
                                    .setTraceablePlatformAgentVersion("1.30.2-rc.3")))
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
            blockingRulesSupplier));
  }

  @Test
  void ruleFormationMixed() {
    // Different libtraceable version
    assertEquals(
        List.of(
            getV4Response(
                List.of(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder().setLibtraceableVersion("0.1.98-rc.139"))
                        .build(),
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder().setLibtraceableVersion("0.1.98-rc.140"))
                        .build())),
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
            blockingRulesSupplier));

    // Same hash for all
    assertEquals(
        List.of(
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
                .build(),
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
            blockingRulesSupplier));

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
            blockingRulesSupplier));
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
