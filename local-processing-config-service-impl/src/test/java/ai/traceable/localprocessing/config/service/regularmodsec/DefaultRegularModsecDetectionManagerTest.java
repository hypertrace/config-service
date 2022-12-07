package ai.traceable.localprocessing.config.service.regularmodsec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesResponse;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.RegularModsecDetectionRules;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultRegularModsecDetectionManagerTest {
  private UuidGenerator uuidGenerator;
  private AnomalyModsecConfigServiceBlockingStub configServiceBlockingStub;
  private RegularModsecDetectionManager regularModsecDetectionManager;

  @BeforeEach
  void setup() {
    configServiceBlockingStub = mock(AnomalyModsecConfigServiceBlockingStub.class);
    FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);
    when(featureCachingClient.isTpaModSecProcessingDisabled(
            argThat(requestContext -> "tenant1".equals(requestContext.getTenantId().orElse("")))))
        .thenReturn(true);
    uuidGenerator = new UuidGenerator();
    regularModsecDetectionManager =
        new DefaultRegularModsecDetectionManager(configServiceBlockingStub, uuidGenerator);
  }

  @Test
  void testDetectionRules() {
    RegularModsecDetectionRules expectedModsecDetectionRules =
        RegularModsecDetectionRules.newBuilder()
            .setRegularModsecDetectionRulesBlob("Tester rule blob")
            .setHash(uuidGenerator.generateId("Tester rule blob"))
            .build();

    when(configServiceBlockingStub.getModsecCrsRules(any()))
        .thenReturn(
            GetModsecCrsRulesResponse.newBuilder()
                .addAllModsecCrsRules(
                    List.of(
                        ModsecCrsRulesData.newBuilder()
                            .setModsecCrsRulesBlob("Tester rule blob")
                            .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE)
                            .build()))
                .build());

    // When hash does not match we expect the blob
    assertEquals(
        expectedModsecDetectionRules,
        regularModsecDetectionManager.getDetectionRules(RequestContext.forTenantId("test"), ""));

    // When hash matches we don't expect the blob
    assertEquals(
        RegularModsecDetectionRules.newBuilder()
            .setHash(uuidGenerator.generateId("Tester rule blob"))
            .build(),
        regularModsecDetectionManager.getDetectionRules(
            RequestContext.forTenantId("test"), uuidGenerator.generateId("Tester rule blob")));
  }

  @Test
  void propagateErrors() {
    when(configServiceBlockingStub.getModsecCrsRules(any())).thenThrow(RuntimeException.class);
    assertThrows(
        RuntimeException.class,
        () ->
            regularModsecDetectionManager.getDetectionRules(
                RequestContext.forTenantId("test"), ""));
  }
}
