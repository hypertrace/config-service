package ai.traceable.blocking.config.service.v1.customsignature;

import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_ALLOW;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_DETECTION_AND_BLOCKING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.v1.CustomModsecBlockingRules;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CustomModesecBlockingManagerTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String ENVIRONMENT_ID = "env-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  private final UuidGenerator uuidGenerator = new UuidGenerator();
  private CustomModsecBlockingManager customModsecBlockingManager;

  @BeforeEach
  void setup() {
    CustomSignatureConfigServiceBlockingStub customSignatureConfigServiceBlockingStub =
        mock(CustomSignatureConfigServiceBlockingStub.class);

    customModsecBlockingManager =
        new DefaultCustomModsecBlockingManager(
            customSignatureConfigServiceBlockingStub, uuidGenerator);

    doReturn(
            GetCustomSignatureModsecRulesResponse.newBuilder()
                .setModsecRulesBlob("testblob")
                .build())
        .when(customSignatureConfigServiceBlockingStub)
        .getCustomSignatureModsecRules(
            GetCustomSignatureModsecRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .addEventTypes(EVENT_TYPE_DETECTION_AND_BLOCKING)
                        .addEventTypes(EVENT_TYPE_ALLOW)
                        .setDisabled(false))
                .build());

    doReturn(
            GetCustomSignatureModsecRulesResponse.newBuilder()
                .setModsecRulesBlob("testblob-env-scoped")
                .build())
        .when(customSignatureConfigServiceBlockingStub)
        .getCustomSignatureModsecRules(
            GetCustomSignatureModsecRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .addEventTypes(EVENT_TYPE_DETECTION_AND_BLOCKING)
                        .addEventTypes(EVENT_TYPE_ALLOW)
                        .setDisabled(false)
                        .setRuleScope(
                            RuleScope.newBuilder()
                                .setEnvironmentScope(
                                    EnvironmentScope.newBuilder()
                                        .addEnvironmentIds(ENVIRONMENT_ID))))
                .build());
  }

  @Test
  void testEnabledBlockingRules() {
    CustomModsecBlockingRules blockingRules =
        customModsecBlockingManager.getEnabledBlockingRules(REQUEST_CONTEXT, "", Optional.empty());
    assertEquals("testblob", blockingRules.getCustomModsecRulesBlob());
    assertEquals(uuidGenerator.generateId("testblob"), blockingRules.getHash());

    blockingRules =
        customModsecBlockingManager.getEnabledBlockingRules(
            REQUEST_CONTEXT, blockingRules.getHash(), Optional.empty());
    assertFalse(blockingRules.hasCustomModsecRulesBlob());
    assertEquals(uuidGenerator.generateId("testblob"), blockingRules.getHash());

    blockingRules =
        customModsecBlockingManager.getEnabledBlockingRules(
            REQUEST_CONTEXT, "", Optional.of(ENVIRONMENT_ID));
    assertEquals("testblob-env-scoped", blockingRules.getCustomModsecRulesBlob());
    assertEquals(uuidGenerator.generateId("testblob-env-scoped"), blockingRules.getHash());

    blockingRules =
        customModsecBlockingManager.getEnabledBlockingRules(
            REQUEST_CONTEXT, blockingRules.getHash(), Optional.of(ENVIRONMENT_ID));
    assertFalse(blockingRules.hasCustomModsecRulesBlob());
    assertEquals(uuidGenerator.generateId("testblob-env-scoped"), blockingRules.getHash());
  }
}
