package ai.traceable.blocking.config.service.v1.blockingmodsec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesResponse;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import ai.traceable.blocking.config.service.v1.SafeCrsBlockingRules;
import ai.traceable.config.utils.UuidGenerator;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ModsecBlockingManagerTest {
  private final UuidGenerator uuidGenerator = new UuidGenerator();
  private AnomalyModsecConfigServiceBlockingStub configServiceBlockingStub;
  private ModsecBlockingManager modsecBlockingManager;

  @BeforeEach
  void setup() {
    configServiceBlockingStub = mock(AnomalyModsecConfigServiceBlockingStub.class);
    modsecBlockingManager =
        new DefaultModsecBlockingManager(configServiceBlockingStub, uuidGenerator);
  }

  @Test
  void testDetectionRules() {
    SafeCrsBlockingRules expectedModsecRules =
        SafeCrsBlockingRules.newBuilder()
            .setSafeCrsRulesBlob("Tester rule blob")
            .setHash(uuidGenerator.generateId("Tester rule blob"))
            .build();

    when(configServiceBlockingStub.getModsecCrsRules(any()))
        .thenReturn(
            GetModsecCrsRulesResponse.newBuilder()
                .addAllModsecCrsRules(
                    List.of(
                        ModsecCrsRulesData.newBuilder()
                            .setModsecCrsRulesBlob("Tester rule blob")
                            .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_BLOCK)
                            .build()))
                .build());

    // When hash does not match we expect the blob
    assertEquals(expectedModsecRules, modsecBlockingManager.getBlockingRules(""));

    // When hash matches we don't expect the blob
    assertEquals(
        SafeCrsBlockingRules.newBuilder()
            .setHash(uuidGenerator.generateId("Tester rule blob"))
            .build(),
        modsecBlockingManager.getBlockingRules(uuidGenerator.generateId("Tester rule blob")));
  }

  @Test
  void propagateErrors() {
    when(configServiceBlockingStub.getModsecCrsRules(any())).thenThrow(RuntimeException.class);
    assertThrows(RuntimeException.class, () -> modsecBlockingManager.getBlockingRules(""));
  }

  @Test
  void handlesErrors() {
    when(configServiceBlockingStub.getModsecCrsRules(any()))
        .thenReturn(
            GetModsecCrsRulesResponse.newBuilder()
                .addAllModsecCrsRules(
                    List.of(
                        ModsecCrsRulesData.newBuilder()
                            .setModsecCrsRulesBlob("Tester rule blob")
                            .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_REGULAR)
                            .build()))
                .build());

    // When hash does not match we expect the blob
    assertThrows(RuntimeException.class, () -> modsecBlockingManager.getBlockingRules(""));
  }
}
