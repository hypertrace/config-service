package ai.traceable.blocking.config.service.common.rules.fetchers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import ai.traceable.blocking.config.service.common.rules.ModsecRulesData;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceGrpc.DetectionExclusionConfigServiceBlockingStub;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionModsecRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesResponse;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.ModsecBlobData;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
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

public class ExclusionRulesFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
  private ExclusionRulesFetcher exclusionRulesFetcher;

  @BeforeEach
  void setup() {
    DetectionExclusionConfigServiceBlockingStub detectionExclusionConfigServiceBlockingStub =
        mock(DetectionExclusionConfigServiceBlockingStub.class, Answers.RETURNS_SELF);

    exclusionRulesFetcher =
        new ExclusionRulesFetcher(
            detectionExclusionConfigServiceBlockingStub, ClientConfig.DEFAULT);

    doReturn(
            GetExclusionModsecRulesResponse.newBuilder()
                .setModsecDirectivesBlob("testDirectives")
                .addModsecRules(
                    DetectionExclusionModsecRule.newBuilder()
                        .setRule(DetectionExclusionRule.newBuilder().setId("id1")))
                .addModsecRules(
                    DetectionExclusionModsecRule.newBuilder()
                        .setRule(DetectionExclusionRule.newBuilder().setId("id2")))
                .addModsecRules(
                    DetectionExclusionModsecRule.newBuilder()
                        .setRule(DetectionExclusionRule.newBuilder().setId("id3")))
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
        .when(detectionExclusionConfigServiceBlockingStub)
        .getExclusionModsecRules(
            GetExclusionModsecRulesRequest.newBuilder()
                .setRulesFilter(
                    GetRulesFilter.newBuilder()
                        .setRuleScope(
                            DetectionExclusionRuleScope.newBuilder()
                                .setEnvironmentScope(EnvironmentScope.getDefaultInstance()))
                        .setDisabled(false)
                        .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_ALLOW)
                        .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_BLOCK)
                        .addAllRuleCreationSources(
                            List.of(
                                RuleSource.RULE_SOURCE_DEFAULT,
                                RuleSource.RULE_SOURCE_CUSTOMER,
                                RuleSource.RULE_SOURCE_TRACEABLE,
                                RuleSource.RULE_SOURCE_SYSTEM,
                                RuleSource.RULE_SOURCE_OLD_API)))
                .addAllServiceNames(List.of("service1", "service2", "service3", "service-x"))
                .build());
  }

  @Test
  void test_fetchExclusionModsecRules() {
    assertTrue(
        exclusionRulesFetcher
            .fetchExclusionModsecRules(REQUEST_CONTEXT, Optional.empty(), Set.of())
            .isEmpty());

    Map<String, ModsecRulesData<DetectionExclusionModsecRule>> exclusionModsecRulesDataMap =
        exclusionRulesFetcher.fetchExclusionModsecRules(
            REQUEST_CONTEXT,
            Optional.empty(),
            new LinkedHashSet<>(List.of("service1", "service2", "service3", "service-x")));

    // no exclusion rules for service-x
    assertEquals(4, exclusionModsecRulesDataMap.size());

    ModsecRulesData<DetectionExclusionModsecRule> exclusionModsecRulesData;

    exclusionModsecRulesData = exclusionModsecRulesDataMap.get("service-x");
    assertTrue(exclusionModsecRulesData.getModsecDirectivesBlob().isEmpty());
    assertTrue(exclusionModsecRulesData.getModsecRulesBlob().isEmpty());
    assertTrue(exclusionModsecRulesData.getRules().isEmpty());

    exclusionModsecRulesData = exclusionModsecRulesDataMap.get("service1");
    assertEquals("testDirectives", exclusionModsecRulesData.getModsecDirectivesBlob());
    assertEquals("blob1", exclusionModsecRulesData.getModsecRulesBlob());
    assertEquals(
        List.of(
            DetectionExclusionModsecRule.newBuilder()
                .setRule(DetectionExclusionRule.newBuilder().setId("id1"))
                .build(),
            DetectionExclusionModsecRule.newBuilder()
                .setRule(DetectionExclusionRule.newBuilder().setId("id2"))
                .build()),
        exclusionModsecRulesData.getRules());

    exclusionModsecRulesData = exclusionModsecRulesDataMap.get("service2");
    assertEquals("testDirectives", exclusionModsecRulesData.getModsecDirectivesBlob());
    assertEquals("blob1", exclusionModsecRulesData.getModsecRulesBlob());
    assertEquals(
        List.of(
            DetectionExclusionModsecRule.newBuilder()
                .setRule(DetectionExclusionRule.newBuilder().setId("id1"))
                .build(),
            DetectionExclusionModsecRule.newBuilder()
                .setRule(DetectionExclusionRule.newBuilder().setId("id2"))
                .build()),
        exclusionModsecRulesData.getRules());

    exclusionModsecRulesData = exclusionModsecRulesDataMap.get("service3");
    assertEquals("testDirectives", exclusionModsecRulesData.getModsecDirectivesBlob());
    assertEquals("blob2", exclusionModsecRulesData.getModsecRulesBlob());
    assertEquals(
        List.of(
            DetectionExclusionModsecRule.newBuilder()
                .setRule(DetectionExclusionRule.newBuilder().setId("id1"))
                .build(),
            DetectionExclusionModsecRule.newBuilder()
                .setRule(DetectionExclusionRule.newBuilder().setId("id3"))
                .build()),
        exclusionModsecRulesData.getRules());
  }
}
