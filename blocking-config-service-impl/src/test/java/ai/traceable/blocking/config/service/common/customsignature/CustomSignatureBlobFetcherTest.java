package ai.traceable.blocking.config.service.common.customsignature;

import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_ALLOW;
import static ai.traceable.customsignature.config.service.v1.EventType.EVENT_TYPE_DETECTION_AND_BLOCKING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
import ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import java.util.Optional;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

public class CustomSignatureBlobFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final Optional<String> ENVIRONMENT_ID = Optional.of("env-id");
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private CustomSignatureBlobFetcher customSignatureBlobFetcher;

  @BeforeEach
  void setup() {
    CustomSignatureConfigServiceBlockingStub customSignatureConfigServiceBlockingStub =
        mock(CustomSignatureConfigServiceBlockingStub.class, Answers.RETURNS_SELF);

    customSignatureBlobFetcher =
        new DefaultCustomSignatureBlobFetcher(
            customSignatureConfigServiceBlockingStub, ClientConfig.DEFAULT);

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
                .setRuleVersion(CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS)
                .setModsecCrsRulesTarget(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING)
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
                                        .addEnvironmentIds(ENVIRONMENT_ID.get()))))
                .setRuleVersion(CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3)
                .setModsecCrsRulesTarget(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING)
                .build());
  }

  @Test
  void testEnabledBlockingRules() {
    String customSignatureRulesBlob =
        customSignatureBlobFetcher.getEnabledCustomSignatureRulesBlob(
            REQUEST_CONTEXT,
            CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS,
            Optional.empty());
    assertEquals("testblob", customSignatureRulesBlob);

    customSignatureRulesBlob =
        customSignatureBlobFetcher.getEnabledCustomSignatureRulesBlob(
            REQUEST_CONTEXT, CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3, ENVIRONMENT_ID);
    assertEquals("testblob-env-scoped", customSignatureRulesBlob);
  }
}
