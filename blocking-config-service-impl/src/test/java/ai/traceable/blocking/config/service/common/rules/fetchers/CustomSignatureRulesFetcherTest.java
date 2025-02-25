package ai.traceable.blocking.config.service.common.rules.fetchers;

import static ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3;
import static ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.CustomSignatureInlineRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
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

public class CustomSignatureRulesFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final Optional<String> ENVIRONMENT_ID = Optional.of("env-id");
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private CustomSignatureRulesFetcher customSignatureRulesFetcher;

  @BeforeEach
  void setup() {
    CustomSignatureConfigServiceBlockingStub customSignatureConfigServiceBlockingStub =
        mock(CustomSignatureConfigServiceBlockingStub.class, Answers.RETURNS_SELF);

    customSignatureRulesFetcher =
        new CustomSignatureRulesFetcher(
            customSignatureConfigServiceBlockingStub, ClientConfig.DEFAULT);

    doReturn(
            GetCustomSignatureModsecRulesResponse.newBuilder()
                .setModsecRulesBlob("testblob")
                .addInlineRules(
                    CustomSignatureInlineRule.newBuilder()
                        .setRule(CustomSignatureRule.newBuilder().setId("ruleId1"))
                        .build())
                .build())
        .when(customSignatureConfigServiceBlockingStub)
        .getCustomSignatureModsecRules(
            GetCustomSignatureModsecRulesRequest.newBuilder()
                .setFilter(GetRulesFilter.newBuilder().setDisabled(false))
                .setRuleVersion(CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS)
                .build());

    doReturn(
            GetCustomSignatureModsecRulesResponse.newBuilder()
                .setModsecRulesBlob("testblob-env-scoped")
                .addInlineRules(
                    CustomSignatureInlineRule.newBuilder()
                        .setRule(CustomSignatureRule.newBuilder().setId("ruleId1-env"))
                        .build())
                .build())
        .when(customSignatureConfigServiceBlockingStub)
        .getCustomSignatureModsecRules(
            GetCustomSignatureModsecRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .setDisabled(false)
                        .setRuleScope(
                            RuleScope.newBuilder()
                                .setEnvironmentScope(
                                    EnvironmentScope.newBuilder()
                                        .addEnvironmentIds(ENVIRONMENT_ID.get()))))
                .setRuleVersion(CUSTOM_MODSEC_RULE_VERSION_V3)
                .build());
  }

  @Test
  void test_fetchModsecRules() {
    GetCustomSignatureModsecRulesResponse customSignatureModsecRulesResponse =
        customSignatureRulesFetcher.fetchModsecRules(
            REQUEST_CONTEXT, Optional.empty(), CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS);
    assertEquals("testblob", customSignatureModsecRulesResponse.getModsecRulesBlob());
    assertEquals(1, customSignatureModsecRulesResponse.getInlineRulesCount());
    assertEquals("ruleId1", customSignatureModsecRulesResponse.getInlineRules(0).getRule().getId());

    customSignatureModsecRulesResponse =
        customSignatureRulesFetcher.fetchModsecRules(
            REQUEST_CONTEXT, ENVIRONMENT_ID, CUSTOM_MODSEC_RULE_VERSION_V3);
    assertEquals("testblob-env-scoped", customSignatureModsecRulesResponse.getModsecRulesBlob());
    assertEquals(1, customSignatureModsecRulesResponse.getInlineRulesCount());
    assertEquals(
        "ruleId1-env", customSignatureModsecRulesResponse.getInlineRules(0).getRule().getId());
  }
}
