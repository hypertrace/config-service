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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CustomSignatureBlockingManagerTest {
  private static final String V3_blob = "Tester rule blob v3";
  private static final String V3_seg_arg_blob = "Tester rule blob v3 seg arg limit";
  private static final UuidGenerator uuidGenerator = new UuidGenerator();
  private static final RequestContext requestContext = RequestContext.forTenantId("test-tenant");
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
    doReturn(Map.of("service1", V3_blob, "service2", V3_seg_arg_blob, "service3", V3_seg_arg_blob))
        .when(blockingRulesSupplier)
        .getCustomModsecBlobs(
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS,
            new LinkedHashSet<>(List.of("service1", "service2", "service3")));
    doReturn(Map.of("serviceName", V3_blob))
        .when(blockingRulesSupplier)
        .getCustomModsecBlobs(
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3, Set.of("serviceName"));
  }

  @Test
  void ruleFormationV3Only() {

    // Incorrect libtraceable version
    assertEquals(
        List.of(getV3Response(List.of(getLibtraceableAgentCapabilities("0.0.2.3")), true)),
        manager.generateBlockingElements(
            requestContext,
            List.of(
                getRequestElement(
                    "hg", List.of(getLibtraceableAgentCapabilities("0.0.2.3")), true)),
            blockingRulesSupplier));

    // Old libtraceable version
    assertEquals(
        List.of(
            getV3Response(
                List.of(getServiceLibtraceableAgentCapabilities("serviceName", "0.1.98-rc.110")),
                true)),
        manager.generateBlockingElements(
            requestContext,
            List.of(
                getRequestElement(
                    "random",
                    List.of(
                        getServiceLibtraceableAgentCapabilities("serviceName", "0.1.98-rc.110")),
                    true)),
            blockingRulesSupplier));

    // Same hash for all
    assertEquals(
        List.of(getV3Response(List.of(getLibtraceableAgentCapabilities("0.1.98-rc.138")), false)),
        manager.generateBlockingElements(
            requestContext,
            List.of(
                getRequestElement(V3_HASH, List.of(), true),
                getRequestElement(
                    V3_HASH, List.of(getLibtraceableAgentCapabilities("0.1.98-rc.138")), true)),
            blockingRulesSupplier));

    // Same hash for some
    assertEquals(
        List.of(
            getV3Response(
                List.of(
                    getTpaLibtraceableAgentCapabilities("1.30.2-rc.3", ""),
                    getTraceablePlatformAgentCapabilities("0.1.98-rc.148"),
                    getLibtraceableAgentCapabilities("0.1.98-rc.138")),
                true)),
        manager.generateBlockingElements(
            requestContext,
            List.of(
                getRequestElement(
                    "random",
                    List.of(
                        getTpaLibtraceableAgentCapabilities("1.30.2-rc.3", ""),
                        getTraceablePlatformAgentCapabilities("0.1.98-rc.148")),
                    true),
                getRequestElement(
                    V3_HASH, List.of(getLibtraceableAgentCapabilities("0.1.98-rc.138")), true)),
            blockingRulesSupplier));
  }

  @Test
  void ruleFormationMixed() {
    // Different libtraceable version
    assertEquals(
        List.of(
            getV3SecArgResponse(
                List.of(
                    getLibtraceableAgentCapabilities("0.1.98-rc.139"),
                    getLibtraceableAgentCapabilities("0.1.98-rc.140")),
                true),
            getV3Response(List.of(getLibtraceableAgentCapabilities("0.1.98-rc.138")), true)),
        manager.generateBlockingElements(
            requestContext,
            List.of(
                getRequestElement(
                    "random",
                    List.of(
                        getLibtraceableAgentCapabilities("0.1.98-rc.139"),
                        getLibtraceableAgentCapabilities("0.1.98-rc.140"),
                        getLibtraceableAgentCapabilities("0.1.98-rc.138")),
                    true)),
            blockingRulesSupplier));

    // Same hash for all
    assertEquals(
        List.of(
            getV3SecArgResponse(
                List.of(
                    getLibtraceableAgentCapabilities("0.1.98-rc.139"),
                    getLibtraceableAgentCapabilities("0.1.98-rc.149")),
                false),
            getV3Response(List.of(getLibtraceableAgentCapabilities("0.1.98-rc.138")), false)),
        manager.generateBlockingElements(
            requestContext,
            List.of(
                getRequestElement(
                    V3_SEG_ARG_HASH,
                    List.of(getLibtraceableAgentCapabilities("0.1.98-rc.139")),
                    true),
                getRequestElement(
                    V3_HASH, List.of(getLibtraceableAgentCapabilities("0.1.98-rc.138")), true),
                getRequestElement(
                    V3_SEG_ARG_HASH,
                    List.of(getLibtraceableAgentCapabilities("0.1.98-rc.149")),
                    true)),
            blockingRulesSupplier));

    // Test empty in case request elements are empty
    assertEquals(
        List.of(),
        manager.generateBlockingElements(
            requestContext,
            List.of(
                getRequestElement(
                    V3_HASH,
                    List.of(
                        getLibtraceableAgentCapabilities("0.1.98-rc.138"),
                        getLibtraceableAgentCapabilities("0.1.98-rc.148")),
                    false)),
            blockingRulesSupplier));

    // Test in case of serviceNames present
    assertEquals(
        List.of(
            getV3SecArgResponse(
                List.of(
                    getLibtraceableAgentCapabilities("0.1.98-rc.149"),
                    getServiceLibtraceableAgentCapabilities("service2", "0.1.98-rc.148"),
                    getServiceLibtraceableAgentCapabilities("service3", "0.1.98-rc.148")),
                true),
            getV3Response(
                List.of(getServiceLibtraceableAgentCapabilities("service1", "0.1.98-rc.139")),
                true),
            getV3Response(List.of(getLibtraceableAgentCapabilities("0.1.98-rc.138")), false)),
        manager.generateBlockingElements(
            requestContext,
            List.of(
                getRequestElement(
                    V3_SEG_ARG_HASH,
                    List.of(
                        getServiceLibtraceableAgentCapabilities("service1", "0.1.98-rc.139"),
                        getLibtraceableAgentCapabilities("0.1.98-rc.149")),
                    true),
                getRequestElement(
                    V3_HASH,
                    List.of(
                        getServiceLibtraceableAgentCapabilities("service2", "0.1.98-rc.148"),
                        getServiceLibtraceableAgentCapabilities("service3", "0.1.98-rc.148"),
                        getLibtraceableAgentCapabilities("0.1.98-rc.138")),
                    true)),
            blockingRulesSupplier));
  }

  private AgentCapabilities getLibtraceableAgentCapabilities(String libtraceableVersion) {
    return AgentCapabilities.newBuilder()
        .addComponents(Component.newBuilder().setLibtraceableVersion(libtraceableVersion))
        .build();
  }

  private AgentCapabilities getTraceablePlatformAgentCapabilities(String tpaVersion) {
    return AgentCapabilities.newBuilder()
        .addComponents(Component.newBuilder().setTraceablePlatformAgentVersion(tpaVersion))
        .build();
  }

  private AgentCapabilities getTpaLibtraceableAgentCapabilities(
      String tpaVersion, String libtraceableVersion) {
    return AgentCapabilities.newBuilder()
        .addComponents(Component.newBuilder().setTraceablePlatformAgentVersion(tpaVersion))
        .addComponents(Component.newBuilder().setLibtraceableVersion(libtraceableVersion))
        .build();
  }

  private AgentCapabilities getServiceLibtraceableAgentCapabilities(
      String serviceName, String libtraceableVersion) {
    return AgentCapabilities.newBuilder()
        .addComponents(Component.newBuilder().setServiceName(serviceName))
        .addComponents(Component.newBuilder().setLibtraceableVersion(libtraceableVersion))
        .build();
  }

  private BlockingConfigRequestElement getRequestElement(
      String previousHash,
      List<AgentCapabilities> agentCapabilities,
      boolean setCustomSignatureRequest) {
    BlockingConfigRequestElement.Builder builder =
        BlockingConfigRequestElement.newBuilder()
            .setPreviousHash(previousHash)
            .addAllSupportedAgentCapabilities(agentCapabilities);
    if (setCustomSignatureRequest) {
      builder.setCustomSignatureBlockingRulesRequest(
          CustomSignatureBlockingRulesRequest.getDefaultInstance());
    }
    return builder.build();
  }

  private BlockingConfigResponseElement getV3Response(
      List<AgentCapabilities> agentCapabilities, boolean setBlob) {
    return BlockingConfigResponseElement.newBuilder()
        .setHash(V3_HASH)
        .addAllAgentCapabilities(agentCapabilities)
        .setCustomSignatureBlockingRules(
            setBlob
                ? CustomSignatureBlockingRules.newBuilder()
                    .setCustomSignatureRulesBlob(V3_blob)
                    .build()
                : CustomSignatureBlockingRules.getDefaultInstance())
        .build();
  }

  private BlockingConfigResponseElement getV3SecArgResponse(
      List<AgentCapabilities> agentCapabilities, boolean setBlob) {
    return BlockingConfigResponseElement.newBuilder()
        .setHash(V3_SEG_ARG_HASH)
        .addAllAgentCapabilities(agentCapabilities)
        .setCustomSignatureBlockingRules(
            setBlob
                ? CustomSignatureBlockingRules.newBuilder()
                    .setCustomSignatureRulesBlob(V3_seg_arg_blob)
                    .build()
                : CustomSignatureBlockingRules.getDefaultInstance())
        .build();
  }
}
