package ai.traceable.config.service.migration;

import static ai.traceable.detection.exclusion.config.service.v1.RuleSource.RULE_SOURCE_CUSTOMER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClientConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceGrpc;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import ai.traceable.detection.exclusion.config.service.v1.GetDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.IpAddressCondition;
import ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionAuditHelper;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesStore;
import com.google.protobuf.Value;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Objects;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DetectionExclusionAllowOnlyPlatformRemovalMigrationConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static DetectionExclusionConfigServiceGrpc.DetectionExclusionConfigServiceBlockingStub
      detectionExclusionConfigServiceBlockingStub;
  private static DetectionExclusionRulesStore detectionExclusionRulesStore;

  private static final String TENANT_ID_1 = "tenant-id-1";
  private static final String TENANT_ID_2 = "tenant-id-2";
  private static final RequestContext REQUEST_CONTEXT_1 = RequestContext.forTenantId(TENANT_ID_1);
  private static final RequestContext REQUEST_CONTEXT_2 = RequestContext.forTenantId(TENANT_ID_2);
  private static final String APPLICATION_CONFIG =
      "configs/traceable-config-service/application.conf";

  @BeforeAll
  static void init() {
    detectionExclusionConfigServiceBlockingStub =
        DetectionExclusionConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());

    ConfigChangeEventGenerator configChangeEventGenerator =
        new ConfigChangeEventGenerator() {
          @Override
          public void sendCreateNotification(
              RequestContext requestContext, String configType, Value config) {
            // no-op for tests
          }

          @Override
          public void sendDeleteNotification(
              RequestContext requestContext, String configType, Value config) {
            // no-op for tests
          }

          @Override
          public void sendUpdateNotification(
              RequestContext requestContext,
              String configType,
              Value prevConfig,
              Value latestConfig) {
            // no-op for tests
          }

          @Override
          public void sendCreateNotification(
              RequestContext requestContext, String configType, String context, Value config) {
            // no-op for tests
          }

          @Override
          public void sendDeleteNotification(
              RequestContext requestContext, String configType, String context, Value config) {
            // no-op for tests
          }

          @Override
          public void sendUpdateNotification(
              RequestContext requestContext,
              String configType,
              String context,
              Value prevConfig,
              Value latestConfig) {
            // no-op for tests
          }
        };

    DetectionExclusionConfigServiceConfig detectionExclusionConfigServiceConfig =
        new DetectionExclusionConfigServiceConfig(
            ConfigFactory.parseURL(
                Objects.requireNonNull(
                    DetectionExclusionRuleEvaluationPointsMigrationConfigServiceIntegrationTest
                        .class
                        .getClassLoader()
                        .getResource(APPLICATION_CONFIG))));

    FeatureCachingClientConfig featureCachingClientConfig =
        FeatureCachingClientConfig.builder()
            .host("localhost")
            .port(60097)
            .requestTimeout(java.time.Duration.ofSeconds(10))
            .refreshDuration(java.time.Duration.ofMinutes(5))
            .expirationDuration(java.time.Duration.ofMinutes(15))
            .threadPoolSize(2)
            .build();

    FeatureCachingClient featureCachingClient =
        new FeatureCachingClient(featureCachingClientConfig, channelRegistry);

    detectionExclusionRulesStore =
        new DetectionExclusionRulesStore(
            configServiceBlockingStub,
            configChangeEventGenerator,
            featureCachingClient,
            detectionExclusionConfigServiceConfig,
            new DetectionExclusionAuditHelper(detectionExclusionConfigServiceConfig));
  }

  @Test
  void testMigrationForAllowOnlyPlatformRemoval() {
    // case-1: rule has rule evaluation points other than PLATFORM
    DetectionExclusionRule createdRule1 = createDetectionExclusionRule1();
    String createdRuleId1 = createdRule1.getId();

    List<DetectionExclusionRule> fetchedRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID_1,
                () ->
                    detectionExclusionConfigServiceBlockingStub.getDetectionExclusionRules(
                        getDetectionExclusionRulesRequest(createdRuleId1)))
            .getRulesList();

    DetectionExclusionRule fetchedRule1 = fetchedRules.get(0);
    assertEquals(1, fetchedRule1.getRuleInfo().getRuleEvaluationPointsList().size());
    assertTrue(
        fetchedRule1
            .getRuleInfo()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE));

    // case-2: rule has no rule evaluation points other than PLATFORM
    DetectionExclusionRule createdRule2 = createDetectionExclusionRule2();
    String createdRuleId2 = createdRule2.getId();

    fetchedRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID_2,
                () ->
                    detectionExclusionConfigServiceBlockingStub.getDetectionExclusionRules(
                        getDetectionExclusionRulesRequest(createdRuleId2)))
            .getRulesList();

    DetectionExclusionRule fetchedRule2 = fetchedRules.get(0);
    assertEquals(1, fetchedRule2.getRuleInfo().getRuleEvaluationPointsList().size());
    assertTrue(
        fetchedRule2
            .getRuleInfo()
            .getRuleEvaluationPointsList()
            .contains(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT));
  }

  private DetectionExclusionRule createDetectionExclusionRule1() {
    DetectionExclusionRule detectionExclusionRule =
        DetectionExclusionRule.newBuilder()
            .setId("rule-id-1")
            .setRuleScope(getDetectionExclusionRuleScope())
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("rule-name-1")
                    .setDescription("rule-description-1")
                    .setRuleStatus(getDetectionExclusionRuleStatus())
                    .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_ALLOW)
                    .addConditions(getIpAddressCondition())
                    .addAllRuleEvaluationPoints(
                        List.of(
                            RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE,
                            RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)))
            .build();
    detectionExclusionRulesStore.upsertObjects(REQUEST_CONTEXT_1, List.of(detectionExclusionRule));
    return detectionExclusionRule;
  }

  private DetectionExclusionRule createDetectionExclusionRule2() {
    DetectionExclusionRule detectionExclusionRule =
        DetectionExclusionRule.newBuilder()
            .setId("rule-id-2")
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("rule-name-2")
                    .setDescription("rule-description-2")
                    .setRuleStatus(getDetectionExclusionRuleStatus())
                    .addExclusionTargets(ExclusionTarget.EXCLUSION_TARGET_ALLOW)
                    .addConditions(getIpAddressCondition())
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM))
            .setRuleScope(getDetectionExclusionRuleScope())
            .build();
    detectionExclusionRulesStore.upsertObjects(REQUEST_CONTEXT_2, List.of(detectionExclusionRule));
    return detectionExclusionRule;
  }

  private GetDetectionExclusionRulesRequest getDetectionExclusionRulesRequest(String ruleId) {
    return GetDetectionExclusionRulesRequest.newBuilder()
        .setFilter(GetRulesFilter.newBuilder().addRuleIds(ruleId))
        .build();
  }

  private DetectionExclusionCondition getIpAddressCondition() {
    return DetectionExclusionCondition.newBuilder()
        .setIpAddressCondition(IpAddressCondition.newBuilder().addIpAddresses("1.2.3.4"))
        .build();
  }

  private DetectionExclusionRuleScope getDetectionExclusionRuleScope() {
    return DetectionExclusionRuleScope.newBuilder()
        .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-id"))
        .build();
  }

  private DetectionExclusionRuleStatus getDetectionExclusionRuleStatus() {
    return DetectionExclusionRuleStatus.newBuilder()
        .setRuleCreationSource(RULE_SOURCE_CUSTOMER)
        .build();
  }
}
