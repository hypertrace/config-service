package ai.traceable.blocking.config.service.common.modsec;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesResponse;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

class BlockingModsecBlobFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final Optional<String> environmentId = Optional.of("env-id");
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private static final String V3_blob = "Tester rule blob v3";
  private static final String V3_seg_arg_blob = "Tester rule blob v3 seg arg limit";

  private AnomalyModsecConfigServiceBlockingStub configServiceBlockingStub;
  private BlockingModsecBlobFetcher blockingModsecBlobFetcher;

  @BeforeEach
  void setup() {
    configServiceBlockingStub =
        mock(AnomalyModsecConfigServiceBlockingStub.class, Answers.RETURNS_SELF);
    blockingModsecBlobFetcher =
        new BlockingModsecBlobFetcher(
            configServiceBlockingStub, ClientConfig.DEFAULT, ConfigFactory.empty());
  }

  @Test
  void testBlobFetcher() {
    when(configServiceBlockingStub.getModsecCrsRules(
            GetModsecCrsRulesRequest.newBuilder()
                .addAllSubRuleTypes(List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK))
                .setRuleVersion(ModsecRuleVersion.MODSEC_RULE_VERSION_V3)
                .setRemoveDisabledRules(true)
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setEnvironmentScope(
                            AnomalyEnvironmentScope.newBuilder()
                                .setEnvironmentId(environmentId.orElse(""))))
                .build()))
        .thenReturn(
            GetModsecCrsRulesResponse.newBuilder()
                .addAllModsecCrsRules(
                    List.of(
                        ModsecCrsRulesData.newBuilder()
                            .setModsecCrsRulesBlob(V3_blob)
                            .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                            .build()))
                .build());

    when(configServiceBlockingStub.getModsecCrsRules(
            GetModsecCrsRulesRequest.newBuilder()
                .addAllSubRuleTypes(List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK))
                .setRuleVersion(ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS)
                .setRemoveDisabledRules(true)
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setEnvironmentScope(
                            AnomalyEnvironmentScope.newBuilder()
                                .setEnvironmentId(environmentId.orElse(""))))
                .build()))
        .thenReturn(
            GetModsecCrsRulesResponse.newBuilder()
                .addAllModsecCrsRules(
                    List.of(
                        ModsecCrsRulesData.newBuilder()
                            .setModsecCrsRulesBlob(V3_seg_arg_blob)
                            .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                            .build()))
                .build());

    assertEquals(
        V3_blob,
        blockingModsecBlobFetcher.getEnabledRulesBlob(
            REQUEST_CONTEXT, ModsecRuleVersion.MODSEC_RULE_VERSION_V3, environmentId));
    assertEquals(
        V3_seg_arg_blob,
        blockingModsecBlobFetcher.getEnabledRulesBlob(
            REQUEST_CONTEXT,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS,
            environmentId));
  }

  @Test
  void testAggregatedBlobFetcher() {
    blockingModsecBlobFetcher =
        new BlockingModsecBlobFetcher(
            configServiceBlockingStub,
            ClientConfig.DEFAULT,
            ConfigFactory.parseString("enableNewModsecCrsBlockingFlow = true"));

    when(configServiceBlockingStub.getModsecCrsRules(
            GetModsecCrsRulesRequest.newBuilder()
                .setTarget(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING)
                .setRuleVersion(ModsecRuleVersion.MODSEC_RULE_VERSION_V3)
                .setRemoveDisabledRules(true)
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setEnvironmentScope(
                            AnomalyEnvironmentScope.newBuilder()
                                .setEnvironmentId(environmentId.orElse(""))))
                .build()))
        .thenReturn(
            GetModsecCrsRulesResponse.newBuilder()
                .setAggregatedModsecCrsRulesBlob(V3_blob)
                .build());

    when(configServiceBlockingStub.getModsecCrsRules(
            GetModsecCrsRulesRequest.newBuilder()
                .setTarget(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING)
                .setRuleVersion(ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS)
                .setRemoveDisabledRules(true)
                .setConfigScope(
                    AnomalyConfigScope.newBuilder()
                        .setEnvironmentScope(
                            AnomalyEnvironmentScope.newBuilder()
                                .setEnvironmentId(environmentId.orElse(""))))
                .build()))
        .thenReturn(
            GetModsecCrsRulesResponse.newBuilder()
                .setAggregatedModsecCrsRulesBlob(V3_seg_arg_blob)
                .build());

    assertEquals(
        V3_blob,
        blockingModsecBlobFetcher.getEnabledRulesBlob(
            REQUEST_CONTEXT, ModsecRuleVersion.MODSEC_RULE_VERSION_V3, environmentId));
    assertEquals(
        V3_seg_arg_blob,
        blockingModsecBlobFetcher.getEnabledRulesBlob(
            REQUEST_CONTEXT,
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS,
            environmentId));
  }

  @Test
  void propagateErrors() {
    when(configServiceBlockingStub.getModsecCrsRules(any())).thenThrow(RuntimeException.class);
    assertThrows(
        RuntimeException.class,
        () ->
            blockingModsecBlobFetcher.getEnabledRulesBlob(
                REQUEST_CONTEXT, ModsecRuleVersion.MODSEC_RULE_VERSION_V3, environmentId));
  }
}
