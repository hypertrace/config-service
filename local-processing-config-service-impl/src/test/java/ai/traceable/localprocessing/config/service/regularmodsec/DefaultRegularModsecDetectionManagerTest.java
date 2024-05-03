package ai.traceable.localprocessing.config.service.regularmodsec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalySubRuleType;
import ai.traceable.anomaly.config.service.v1.modsec.AnomalyModsecConfigServiceGrpc.AnomalyModsecConfigServiceBlockingStub;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesRequest;
import ai.traceable.anomaly.config.service.v1.modsec.GetModsecCrsRulesResponse;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesData;
import ai.traceable.anomaly.config.service.v1.modsec.ModsecRuleVersion;
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
    // customer scope
    verifyDetectionRules(ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED, false, false, "");
    verifyDetectionRules(ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3, true, false, "");
    verifyDetectionRules(ModsecRuleVersion.MODSEC_RULE_VERSION_SENSITIVE_AGENT_V3, false, true, "");
    verifyDetectionRules(
        ModsecRuleVersion.MODSEC_RULE_VERSION_SENSITIVE_AGENT_CORAZA_V3, true, true, "");
    // environment scope
    verifyDetectionRules(
        ModsecRuleVersion.MODSEC_RULE_VERSION_UNSPECIFIED, false, false, "environmentId");
    verifyDetectionRules(
        ModsecRuleVersion.MODSEC_RULE_VERSION_CORAZA_V3, true, false, "environmentId");
    verifyDetectionRules(
        ModsecRuleVersion.MODSEC_RULE_VERSION_SENSITIVE_AGENT_V3, false, true, "environmentId");
    verifyDetectionRules(
        ModsecRuleVersion.MODSEC_RULE_VERSION_SENSITIVE_AGENT_CORAZA_V3,
        true,
        true,
        "environmentId");
  }

  private void verifyDetectionRules(
      ModsecRuleVersion version,
      boolean shouldUseCoraza,
      boolean shouldHideMatchValueInCrsMsg,
      String environmentId) {
    String blob = version + " blob";
    String hash = uuidGenerator.generateId(blob);
    RegularModsecDetectionRules expectedModsecDetectionRules =
        RegularModsecDetectionRules.newBuilder()
            .setRegularModsecDetectionRulesBlob(blob)
            .setHash(hash)
            .build();
    RequestContext requestContext = RequestContext.forTenantId("test");
    AnomalyConfigScope configScope =
        environmentId.isEmpty()
            ? AnomalyConfigScope.newBuilder()
                .setCustomerScope(AnomalyCustomerScope.getDefaultInstance())
                .build()
            : AnomalyConfigScope.newBuilder()
                .setEnvironmentScope(
                    AnomalyEnvironmentScope.newBuilder().setEnvironmentId(environmentId))
                .build();

    when(configServiceBlockingStub.getModsecCrsRules(
            GetModsecCrsRulesRequest.newBuilder()
                .addAllSubRuleTypes(List.of(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE))
                .setRemoveDisabledRules(true)
                .setRuleVersion(version)
                .setConfigScope(configScope)
                .build()))
        .thenReturn(
            GetModsecCrsRulesResponse.newBuilder()
                .addAllModsecCrsRules(
                    List.of(
                        ModsecCrsRulesData.newBuilder()
                            .setModsecCrsRulesBlob(blob)
                            .setSubRuleType(AnomalySubRuleType.ANOMALY_SUB_RULE_TYPE_SAFE)
                            .build()))
                .build());

    // When hash does not match we expect the blob
    assertEquals(
        expectedModsecDetectionRules,
        regularModsecDetectionManager.getDetectionRules(
            requestContext, "", shouldUseCoraza, shouldHideMatchValueInCrsMsg, environmentId));

    // When hash matches we don't expect the blob
    assertEquals(
        expectedModsecDetectionRules.toBuilder().clearRegularModsecDetectionRulesBlob().build(),
        regularModsecDetectionManager.getDetectionRules(
            requestContext, hash, shouldUseCoraza, shouldHideMatchValueInCrsMsg, environmentId));
  }

  @Test
  void propagateErrors() {
    when(configServiceBlockingStub.getModsecCrsRules(any())).thenThrow(RuntimeException.class);
    assertThrows(
        RuntimeException.class,
        () ->
            regularModsecDetectionManager.getDetectionRules(
                RequestContext.forTenantId("test"), "", false, false, "environmentId"));
  }
}
