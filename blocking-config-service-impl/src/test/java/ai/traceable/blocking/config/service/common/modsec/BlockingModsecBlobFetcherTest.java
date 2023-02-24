package ai.traceable.blocking.config.service.common.modsec;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesResponse;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockingModsecBlobFetcherTest {

  private static final String V3_blob = "Tester rule blob v3";
  private static final String V3_seg_arg_blob = "Tester rule blob v3 seg arg limit";

  private AnomalyModsecConfigServiceBlockingStub configServiceBlockingStub;
  private BlockingModsecBlobFetcher blockingModsecBlobFetcher;

  @BeforeEach
  void setup() {
    configServiceBlockingStub = mock(AnomalyModsecConfigServiceBlockingStub.class);
    blockingModsecBlobFetcher = new BlockingModsecBlobFetcher(configServiceBlockingStub);
  }

  @Test
  void testBlobFetcher() {
    when(configServiceBlockingStub.getModsecCrsRules(
            GetModsecCrsRulesRequest.newBuilder()
                .addAllSubRuleTypes(List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK))
                .setRuleVersion(ModsecRuleVersion.MODSEC_RULE_VERSION_V3)
                .setRemoveDisabledRules(true)
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
        blockingModsecBlobFetcher.getBlockingModsecBlob(ModsecRuleVersion.MODSEC_RULE_VERSION_V3));
    assertEquals(
        V3_seg_arg_blob,
        blockingModsecBlobFetcher.getBlockingModsecBlob(
            ModsecRuleVersion.MODSEC_RULE_VERSION_V3_SECARG_LIMITS));
  }

  @Test
  void propagateErrors() {
    when(configServiceBlockingStub.getModsecCrsRules(any())).thenThrow(RuntimeException.class);
    assertThrows(
        RuntimeException.class,
        () ->
            blockingModsecBlobFetcher.getBlockingModsecBlob(
                ModsecRuleVersion.MODSEC_RULE_VERSION_V3));
  }
}
