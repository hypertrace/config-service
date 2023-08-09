package ai.traceable.blocking.config.service.common.rules.fetchers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.ratelimiting.config.service.v2.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesRequest;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.ModsecBlobData;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingConfigServiceGrpc.RateLimitingConfigServiceBlockingStub;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingModsecRule;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DlpRulesFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private DlpRulesFetcher dlpRulesFetcher;

  @BeforeEach
  void setup() {
    RateLimitingConfigServiceBlockingStub rateLimitingConfigServiceBlockingStub =
        mock(RateLimitingConfigServiceBlockingStub.class);

    dlpRulesFetcher = new DlpRulesFetcher(rateLimitingConfigServiceBlockingStub);

    doReturn(
            GetRateLimitingRuleModsecRulesResponse.newBuilder()
                .setModsecDirectivesBlob("testDirectives")
                .addRules(RateLimitingModsecRule.newBuilder().setId("id1"))
                .addRules(RateLimitingModsecRule.newBuilder().setId("id2"))
                .addRules(RateLimitingModsecRule.newBuilder().setId("id3"))
                .addModsecBlobsData(
                    ModsecBlobData.newBuilder()
                        .setModsecBlob("blob1")
                        .addRuleIds("id1")
                        .addRuleIds("id2")
                        .addServiceNames("service1")
                        .addServiceNames("service2"))
                .addModsecBlobsData(
                    ModsecBlobData.newBuilder()
                        .setModsecBlob("blob2")
                        .addRuleIds("id1")
                        .addRuleIds("id3")
                        .addServiceNames("service3"))
                .build())
        .when(rateLimitingConfigServiceBlockingStub)
        .getRateLimitingRuleModsecRules(
            GetRateLimitingRuleModsecRulesRequest.newBuilder()
                .setRulesFilter(
                    GetRateLimitingModsecRulesFilter.newBuilder()
                        .addAllRuleActions(
                            List.of(
                                GetRateLimitingModsecRulesFilter.RuleAction
                                    .RULE_ACTION_TRANSACTION_BLOCKED,
                                GetRateLimitingModsecRulesFilter.RuleAction
                                    .RULE_ACTION_TRANSACTION_ALLOWED))
                        .addAllServiceNames(
                            List.of("service1", "service2", "service3", "service-x"))
                        .setScope(
                            RuleConfigScope.newBuilder()
                                .setEnvironmentScope(EnvironmentScope.getDefaultInstance()))
                        .setDisabled(false))
                .build());
  }

  @Test
  void test_fetchDlpModsecRules() {
    assertTrue(
        dlpRulesFetcher
            .fetchDlpModsecRules(REQUEST_CONTEXT, Optional.empty(), List.of())
            .isEmpty());

    Map<String, DlpRulesFetcher.DlpModsecRulesData> dlpModsecRulesDataMap =
        dlpRulesFetcher.fetchDlpModsecRules(
            REQUEST_CONTEXT,
            Optional.empty(),
            List.of("service1", "service2", "service3", "service-x"));
    // no dlp rules for service-x
    assertEquals(4, dlpModsecRulesDataMap.size());

    DlpRulesFetcher.DlpModsecRulesData dlpModsecRulesData;

    dlpModsecRulesData = dlpModsecRulesDataMap.get("service-x");
    assertTrue(dlpModsecRulesData.getModsecDirectivesBlob().isEmpty());
    assertTrue(dlpModsecRulesData.getModsecRulesBlob().isEmpty());
    assertTrue(dlpModsecRulesData.getRules().isEmpty());

    dlpModsecRulesData = dlpModsecRulesDataMap.get("service1");
    assertEquals("testDirectives", dlpModsecRulesData.getModsecDirectivesBlob());
    assertEquals("blob1", dlpModsecRulesData.getModsecRulesBlob());
    assertEquals(
        List.of(
            RateLimitingModsecRule.newBuilder().setId("id1").build(),
            RateLimitingModsecRule.newBuilder().setId("id2").build()),
        dlpModsecRulesData.getRules());

    dlpModsecRulesData = dlpModsecRulesDataMap.get("service2");
    assertEquals("testDirectives", dlpModsecRulesData.getModsecDirectivesBlob());
    assertEquals("blob1", dlpModsecRulesData.getModsecRulesBlob());
    assertEquals(
        List.of(
            RateLimitingModsecRule.newBuilder().setId("id1").build(),
            RateLimitingModsecRule.newBuilder().setId("id2").build()),
        dlpModsecRulesData.getRules());

    dlpModsecRulesData = dlpModsecRulesDataMap.get("service3");
    assertEquals("testDirectives", dlpModsecRulesData.getModsecDirectivesBlob());
    assertEquals("blob2", dlpModsecRulesData.getModsecRulesBlob());
    assertEquals(
        List.of(
            RateLimitingModsecRule.newBuilder().setId("id1").build(),
            RateLimitingModsecRule.newBuilder().setId("id3").build()),
        dlpModsecRulesData.getRules());
  }
}
