package ai.traceable.blocking.config.service.common.rules.fetchers;

import static ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3;
import static ai.traceable.customsignature.config.service.v1.CustomModsecRuleVersion.CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.anomaly.config.service.v1.modsec.ModsecCrsRulesTarget;
import ai.traceable.blocking.config.service.common.rules.ModsecRulesData;
import ai.traceable.customsignature.config.service.v1.CustomSignatureConfigServiceGrpc.CustomSignatureConfigServiceBlockingStub;
import ai.traceable.customsignature.config.service.v1.CustomSignatureInlineRule;
import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.EnvironmentScope;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesRequest;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureModsecRulesResponse;
import ai.traceable.customsignature.config.service.v1.GetRulesFilter;
import ai.traceable.customsignature.config.service.v1.ModsecBlobData;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
                        .setRule(CustomSignatureRule.newBuilder().setId("ruleId1")))
                .build())
        .when(customSignatureConfigServiceBlockingStub)
        .getCustomSignatureModsecRules(
            GetCustomSignatureModsecRulesRequest.newBuilder()
                .setFilter(
                    GetRulesFilter.newBuilder()
                        .setDisabled(false)
                        .setRuleScope(
                            RuleScope.newBuilder()
                                .setEnvironmentScope(EnvironmentScope.getDefaultInstance())))
                .setModsecCrsRulesTarget(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING)
                .setRuleVersion(CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS)
                .build());

    doReturn(
            GetCustomSignatureModsecRulesResponse.newBuilder()
                .setModsecRulesBlob("testblob-env-scoped")
                .addInlineRules(
                    CustomSignatureInlineRule.newBuilder()
                        .setRule(CustomSignatureRule.newBuilder().setId("ruleId1-env")))
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
                .setModsecCrsRulesTarget(ModsecCrsRulesTarget.MODSEC_CRS_RULES_TARGET_TA_BLOCKING)
                .setRuleVersion(CUSTOM_MODSEC_RULE_VERSION_V3)
                .build());

    doReturn(
            GetCustomSignatureModsecRulesResponse.newBuilder()
                .setModsecDirectivesBlob("testDirectives")
                .addInlineRules(
                    CustomSignatureInlineRule.newBuilder()
                        .setRule(CustomSignatureRule.newBuilder().setId("rule1")))
                .addInlineRules(
                    CustomSignatureInlineRule.newBuilder()
                        .setRule(CustomSignatureRule.newBuilder().setId("rule2")))
                .addModsecBlobsData(
                    ModsecBlobData.newBuilder()
                        .setModsecBlob("blob1")
                        .addCustomSignatureRuleIds("rule1")
                        .addServiceNames("service1")
                        .addServiceNames("service2"))
                .addModsecBlobsData(
                    ModsecBlobData.newBuilder()
                        .setModsecBlob("blob2")
                        .addCustomSignatureRuleIds("rule2")
                        .addServiceNames("service3"))
                .build())
        .when(customSignatureConfigServiceBlockingStub)
        .getCustomSignatureModsecRules(
            argThat(
                request ->
                    request.getServiceNamesCount() > 0
                        && request.getRuleVersion()
                            == CUSTOM_MODSEC_RULE_VERSION_V3_SECARG_LIMITS));
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

  @Test
  void test_fetchCustomSignatureModsecRules() {
    // Test with empty service names
    Map<String, ModsecRulesData<CustomSignatureInlineRule>> emptyResult =
        customSignatureRulesFetcher.fetchCustomSignatureInlineRules(
            REQUEST_CONTEXT, ENVIRONMENT_ID, Set.of());
    assert emptyResult != null;
    assertTrue(emptyResult.isEmpty());

    // Test with service names
    Map<String, ModsecRulesData<CustomSignatureInlineRule>> result =
        customSignatureRulesFetcher.fetchCustomSignatureInlineRules(
            REQUEST_CONTEXT,
            ENVIRONMENT_ID,
            new LinkedHashSet<>(List.of("service1", "service2", "service3", "service-x")));

    assert result != null;
    assertEquals(4, result.size());

    ModsecRulesData<CustomSignatureInlineRule> serviceXData = result.get("service-x");
    assertTrue(serviceXData.getModsecDirectivesBlob().isEmpty());
    assertTrue(serviceXData.getModsecRulesBlob().isEmpty());
    assertTrue(serviceXData.getRules().isEmpty());

    ModsecRulesData<CustomSignatureInlineRule> service1Data = result.get("service1");
    assertEquals("testDirectives", service1Data.getModsecDirectivesBlob());
    assertEquals("blob1", service1Data.getModsecRulesBlob());
    assertEquals(1, service1Data.getRules().size());
    assertEquals("rule1", service1Data.getRules().get(0).getRule().getId());

    ModsecRulesData<CustomSignatureInlineRule> service2Data = result.get("service2");
    assertEquals("testDirectives", service2Data.getModsecDirectivesBlob());
    assertEquals("blob1", service2Data.getModsecRulesBlob());
    assertEquals(1, service2Data.getRules().size());
    assertEquals("rule1", service2Data.getRules().get(0).getRule().getId());

    ModsecRulesData<CustomSignatureInlineRule> service3Data = result.get("service3");
    assertEquals("testDirectives", service3Data.getModsecDirectivesBlob());
    assertEquals("blob2", service3Data.getModsecRulesBlob());
    assertEquals(1, service3Data.getRules().size());
    assertEquals("rule2", service3Data.getRules().get(0).getRule().getId());
  }
}
