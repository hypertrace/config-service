package ai.traceable.blocking.config.service.v2.modsec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import ai.traceable.blocking.config.service.common.modsec.BlockingModsecBlobFetcher;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplier;
import ai.traceable.blocking.config.service.common.rules.BlockingRulesSupplierContext;
import ai.traceable.blocking.config.service.v2.AgentCapabilities;
import ai.traceable.blocking.config.service.v2.BlockingConfigManagerBase;
import ai.traceable.blocking.config.service.v2.BlockingConfigRequestElement;
import ai.traceable.blocking.config.service.v2.BlockingConfigResponseElement;
import ai.traceable.blocking.config.service.v2.Component;
import ai.traceable.blocking.config.service.v2.CrsBlockingRules;
import ai.traceable.blocking.config.service.v2.CrsBlockingRulesRequest;
import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.config.utils.UuidGenerator;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ModsecBlockingManagerTest {
  private static final String V3_blob = "Tester rule blob v3";
  private static final String V3_seg_arg_blob = "Tester rule blob v3 seg arg limit";
  private static final UuidGenerator uuidGenerator = new UuidGenerator();
  private static final RequestContext requestContext = RequestContext.forTenantId("TENANT_ID");
  private static final Optional<String> environmentId = Optional.of("environment");
  private static final String V3_HASH = uuidGenerator.generateId(V3_blob);
  private static final String V3_SEG_ARG_HASH = uuidGenerator.generateId(V3_seg_arg_blob);

  private static BlockingConfigManagerBase manager;

  private final BlockingRulesSupplierContext blockingRulesSupplierContext =
      new BlockingRulesSupplierContext(Collections.emptyMap(), null);

  @BeforeAll
  static void setup() {
    BlockingModsecBlobFetcher mockBlockingModsecBlobFetcher = mock(BlockingModsecBlobFetcher.class);
    SemanticVersioningComparator semanticVersioningComparator = new SemanticVersioningComparator();
    manager =
        new ModsecBlockingManager(
            mockBlockingModsecBlobFetcher, uuidGenerator, semanticVersioningComparator);

    doReturn(V3_blob)
        .when(mockBlockingModsecBlobFetcher)
        .getEnabledRulesBlob(
            requestContext, ModsecRuleVersion.MODSEC_RULE_VERSION_V3, environmentId);
    doReturn(V3_seg_arg_blob)
        .when(mockBlockingModsecBlobFetcher)
        .getEnabledRulesBlob(
            requestContext, ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS, environmentId);
  }

  @Test
  void ruleFormationV3Only() {
    // Old libtraceable version
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash(V3_HASH)
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder()
                                .setLibtraceableVersion("0.1.98-rc.138")
                                .setTraceablePlatformAgentVersion("1.31.0")))
                .setCrsBlockingRules(CrsBlockingRules.newBuilder().setCrsRulesBlob(V3_blob))
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random")
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder()
                                    .setLibtraceableVersion("0.1.98-rc.138")
                                    .setTraceablePlatformAgentVersion("1.31.0")))
                    .setCrsBlockingRulesRequest(CrsBlockingRulesRequest.getDefaultInstance())
                    .build()),
            new BlockingRulesSupplier(
                blockingRulesSupplierContext, requestContext, environmentId)));

    // Same hash for all
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash(V3_HASH)
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(Component.newBuilder().setLibtraceableVersion("")))
                .setCrsBlockingRules(CrsBlockingRules.getDefaultInstance())
                .build()),
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
                            .addComponents(Component.newBuilder().setLibtraceableVersion("")))
                    .setCrsBlockingRulesRequest(CrsBlockingRulesRequest.getDefaultInstance())
                    .build()),
            new BlockingRulesSupplier(
                blockingRulesSupplierContext, requestContext, environmentId)));

    // Same hash for some
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash(V3_HASH)
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder().setLibtraceableVersion("0.1.98-rc.128")))
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                .setCrsBlockingRules(CrsBlockingRules.newBuilder().setCrsRulesBlob(V3_blob))
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash("random")
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.128")))
                    .setCrsBlockingRulesRequest(CrsBlockingRulesRequest.getDefaultInstance())
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash(V3_HASH)
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                    .setCrsBlockingRulesRequest(CrsBlockingRulesRequest.getDefaultInstance())
                    .build()),
            new BlockingRulesSupplier(
                blockingRulesSupplierContext, requestContext, environmentId)));
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
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(Component.newBuilder().setLibtraceableVersion("")))
                .setCrsBlockingRules(CrsBlockingRules.newBuilder().setCrsRulesBlob(V3_blob))
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
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(Component.newBuilder().setLibtraceableVersion("")))
                    .setCrsBlockingRulesRequest(CrsBlockingRulesRequest.getDefaultInstance())
                    .build()),
            new BlockingRulesSupplier(
                blockingRulesSupplierContext, requestContext, environmentId)));

    // Same hash for all
    assertEquals(
        List.of(
            BlockingConfigResponseElement.newBuilder()
                .setHash(V3_HASH)
                .addAgentCapabilities(
                    AgentCapabilities.newBuilder()
                        .addComponents(
                            Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                .setCrsBlockingRules(CrsBlockingRules.getDefaultInstance())
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
                .setCrsBlockingRules(CrsBlockingRules.getDefaultInstance())
                .build()),
        manager.generateBlockingElements(
            List.of(
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash(V3_SEG_ARG_HASH)
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.139")))
                    .setCrsBlockingRulesRequest(CrsBlockingRulesRequest.getDefaultInstance())
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash(V3_HASH)
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.138")))
                    .setCrsBlockingRulesRequest(CrsBlockingRulesRequest.getDefaultInstance())
                    .build(),
                BlockingConfigRequestElement.newBuilder()
                    .setPreviousHash(V3_SEG_ARG_HASH)
                    .addSupportedAgentCapabilities(
                        AgentCapabilities.newBuilder()
                            .addComponents(
                                Component.newBuilder().setLibtraceableVersion("0.1.98-rc.149")))
                    .setCrsBlockingRulesRequest(CrsBlockingRulesRequest.getDefaultInstance())
                    .build()),
            new BlockingRulesSupplier(
                blockingRulesSupplierContext, requestContext, environmentId)));

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
            new BlockingRulesSupplier(
                blockingRulesSupplierContext, requestContext, environmentId)));
  }

  private static BlockingConfigResponseElement getV4Response(
      List<AgentCapabilities> agentCapabilities) {
    return BlockingConfigResponseElement.newBuilder()
        .setHash(V3_SEG_ARG_HASH)
        .addAllAgentCapabilities(agentCapabilities)
        .setCrsBlockingRules(CrsBlockingRules.newBuilder().setCrsRulesBlob(V3_seg_arg_blob))
        .build();
  }
}
