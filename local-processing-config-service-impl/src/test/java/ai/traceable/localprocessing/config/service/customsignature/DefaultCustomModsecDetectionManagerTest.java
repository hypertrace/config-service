package ai.traceable.localprocessing.config.service.customsignature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.localprocessing.config.service.utils.UuidGenerator;
import ai.traceable.localprocessing.config.service.v1.CustomModsecDetectionRules;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

class DefaultCustomModsecDetectionManagerTest {
  private UuidGenerator uuidGenerator;
  private CustomSignatureConfigServiceBlockingStub configServiceBlockingStub;
  private CustomModsecDetectionManager customModsecDetectionManager;

  @BeforeEach
  void setup() {
    configServiceBlockingStub =
        mock(CustomSignatureConfigServiceBlockingStub.class, Answers.RETURNS_SELF);
    FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);
    when(featureCachingClient.isTpaModSecProcessingDisabled(
            argThat(requestContext -> "tenant1".equals(requestContext.getTenantId().orElse("")))))
        .thenReturn(true);
    uuidGenerator = new UuidGenerator();
    customModsecDetectionManager =
        new DefaultCustomModsecDetectionManager(
            configServiceBlockingStub, uuidGenerator, ClientConfig.DEFAULT);
  }

  @Test
  void testGetEnabledRules() {
    GetCustomSignatureModsecRulesResponse stubResponse =
        GetCustomSignatureModsecRulesResponse.newBuilder()
            .setModsecRulesBlob("Tester rule blob")
            .build();

    CustomModsecDetectionRules expectedModsecDetectionRules =
        CustomModsecDetectionRules.newBuilder()
            .setCustomModsecDetectionRulesBlob("Tester rule blob")
            .setHash(uuidGenerator.generateId("Tester rule blob"))
            .build();

    when(configServiceBlockingStub.getCustomSignatureModsecRules(any())).thenReturn(stubResponse);

    // When hash does not match we expect the blob
    assertEquals(
        expectedModsecDetectionRules,
        customModsecDetectionManager.getEnabledRules(
            RequestContext.forTenantId("test"), "", false, "environmentId"));

    // When hash matches we don't expect the blob
    assertEquals(
        CustomModsecDetectionRules.newBuilder()
            .setHash(uuidGenerator.generateId("Tester rule blob"))
            .build(),
        customModsecDetectionManager.getEnabledRules(
            RequestContext.forTenantId("test"),
            uuidGenerator.generateId("Tester rule blob"),
            false,
            "environmentId"));
  }

  @Test
  void testGetEnabledRulesWithCoraza() {
    GetCustomSignatureModsecRulesResponse stubResponse =
        GetCustomSignatureModsecRulesResponse.newBuilder()
            .setModsecRulesBlob("Tester rule blob")
            .build();

    CustomModsecDetectionRules expectedModsecDetectionRules =
        CustomModsecDetectionRules.newBuilder()
            .setCustomModsecDetectionRulesBlob("Tester rule blob")
            .setHash(uuidGenerator.generateId("Tester rule blob"))
            .build();

    when(configServiceBlockingStub.getCustomSignatureModsecRules(
            GetCustomSignatureModsecRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .addEventTypes(EventType.EVENT_TYPE_NORMAL_DETECTION)
                        .setDisabled(false)
                        .setRuleScope(
                            RuleScope.newBuilder()
                                .setEnvironmentScope(
                                    EnvironmentScope.newBuilder()
                                        .addEnvironmentIds("environmentId"))))
                .setRuleVersion(CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_CORAZA_V3)
                .setModsecCrsRulesTarget(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TPA_DETECTION)
                .build()))
        .thenReturn(stubResponse);

    // When coraza was enabled, only then we expect the blob
    assertEquals(
        expectedModsecDetectionRules,
        customModsecDetectionManager.getEnabledRules(
            RequestContext.forTenantId("test"), "", true, "environmentId"));
  }

  @Test
  void propagateErrors() {
    when(configServiceBlockingStub.getCustomSignatureModsecRules(any()))
        .thenThrow(RuntimeException.class);
    assertThrows(
        RuntimeException.class,
        () ->
            customModsecDetectionManager.getEnabledRules(
                RequestContext.forTenantId("test"), "", false, "environmentId"));
  }
}
