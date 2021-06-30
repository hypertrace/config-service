package ai.traceable.localprocessing.config.service.customsignature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRuleDetails;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.CustomModsecDetectionRules;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultCustomModsecDetectionManagerTest {
  private UuidGenerator uuidGenerator;
  private CustomSignatureConfigServiceBlockingStub configServiceBlockingStub;
  private CustomModsecDetectionManager customModsecDetectionManager;

  @BeforeEach
  void setup() {
    configServiceBlockingStub = mock(CustomSignatureConfigServiceBlockingStub.class);
    uuidGenerator = new UuidGenerator();
    customModsecDetectionManager =
        new DefaultCustomModsecDetectionManager(configServiceBlockingStub, uuidGenerator);
  }

  @Test
  void testGetEnabledRules() {
    GetCustomSignatureModsecRulesResponse stubResponse =
        GetCustomSignatureModsecRulesResponse.newBuilder()
            .setModsecRulesBlob("Tester rule blob")
            .addAllRules(List.of(CustomSignatureRuleDetails.getDefaultInstance()))
            .build();

    CustomModsecDetectionRules expectedModsecDetectionRules =
        CustomModsecDetectionRules.newBuilder()
            .setCustomModsecDetectionRulesBlob("Tester rule blob")
            .setHash(uuidGenerator.generateId("Tester rule blob"))
            .build();

    when(configServiceBlockingStub.getCustomSignatureModsecRules(any())).thenReturn(stubResponse);

    // When hash does not match we expect the blob
    assertEquals(expectedModsecDetectionRules, customModsecDetectionManager.getEnabledRules(""));

    // When hash matches we don't expect the blob
    assertEquals(
        CustomModsecDetectionRules.newBuilder()
            .setHash(uuidGenerator.generateId("Tester rule blob"))
            .build(),
        customModsecDetectionManager.getEnabledRules(uuidGenerator.generateId("Tester rule blob")));
  }

  @Test
  void propagateErrors() {
    when(configServiceBlockingStub.getCustomSignatureModsecRules(any()))
        .thenThrow(RuntimeException.class);
    assertThrows(RuntimeException.class, () -> customModsecDetectionManager.getEnabledRules(""));
  }
}
