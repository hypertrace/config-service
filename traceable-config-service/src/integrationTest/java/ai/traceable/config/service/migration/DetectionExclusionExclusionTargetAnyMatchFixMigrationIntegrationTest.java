package ai.traceable.config.service.migration;

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
import ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope;
import ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget;
import ai.traceable.detection.exclusion.config.service.v1.GetDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.IpAddressCondition;
import ai.traceable.detection.exclusion.config.service.v1.RegionCondition;
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

public class DetectionExclusionExclusionTargetAnyMatchFixMigrationIntegrationTest
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
  void testMigrationForExclusionTargetAnyMatchFix() {
    DetectionExclusionRuleScope detectionExclusionRuleScope =
        DetectionExclusionRuleScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-id"))
            .build();

    List<ExclusionTarget> exclusionTargetsList1 =
        List.of(ExclusionTarget.EXCLUSION_TARGET_ALLOW, ExclusionTarget.EXCLUSION_TARGET_ALERT);
    List<ExclusionTarget> exclusionTargetsList2 =
        List.of(ExclusionTarget.EXCLUSION_TARGET_BLOCK, ExclusionTarget.EXCLUSION_TARGET_ALERT);
    List<RuleEvaluationPoint> allRuleEvaluationPoints =
        List.of(
            RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM,
            RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE,
            RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT);

    // conditions supported for both EDGE and INLINE_TRACING_AGENT rule evaluation points
    RegionCondition regionCondition =
        RegionCondition.newBuilder()
            .addRegions(RegionCondition.Region.newBuilder().setCountryIsoCode("iso"))
            .build();
    IpAddressCondition ipAddressCondition =
        IpAddressCondition.newBuilder().addIpAddresses("1.2.3.4").build();
    List<DetectionExclusionCondition> detectionExclusionConditions =
        List.of(
            DetectionExclusionCondition.newBuilder()
                .setIpAddressCondition(ipAddressCondition)
                .build(),
            DetectionExclusionCondition.newBuilder().setRegionCondition(regionCondition).build());

    // existing rule eligible for EDGE rule evaluation point upon migration
    DetectionExclusionRule createdRule1 =
        DetectionExclusionRule.newBuilder()
            .setId("rule-id-1")
            .setRuleScope(detectionExclusionRuleScope)
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                    .addAllExclusionTargets(exclusionTargetsList1)
                    .addAllConditions(detectionExclusionConditions))
            .build();

    detectionExclusionRulesStore.upsertObjects(REQUEST_CONTEXT_1, List.of(createdRule1));

    DetectionExclusionRule fetchedRule1 =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID_1,
                () ->
                    detectionExclusionConfigServiceBlockingStub
                        .getDetectionExclusionRules(
                            GetDetectionExclusionRulesRequest.newBuilder()
                                .setFilter(GetRulesFilter.newBuilder().addRuleIds("rule-id-1"))
                                .build())
                        .getRulesList())
            .get(0);
    assertTrue(
        fetchedRule1
            .getRuleInfo()
            .getRuleEvaluationPointsList()
            .containsAll(allRuleEvaluationPoints));

    // existing rule eligible for INLINE_TRACING_AGENT rule evaluation point upon migration
    DetectionExclusionRule createdRule2 =
        DetectionExclusionRule.newBuilder()
            .setId("rule-id-2")
            .setRuleScope(detectionExclusionRuleScope)
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                    .addAllExclusionTargets(exclusionTargetsList2)
                    .addAllConditions(detectionExclusionConditions))
            .build();

    detectionExclusionRulesStore.upsertObjects(REQUEST_CONTEXT_2, List.of(createdRule2));

    DetectionExclusionRule fetchedRule2 =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID_2,
                () ->
                    detectionExclusionConfigServiceBlockingStub
                        .getDetectionExclusionRules(
                            GetDetectionExclusionRulesRequest.newBuilder()
                                .setFilter(GetRulesFilter.newBuilder().addRuleIds("rule-id-2"))
                                .build())
                        .getRulesList())
            .get(0);
    assertTrue(
        fetchedRule2
            .getRuleInfo()
            .getRuleEvaluationPointsList()
            .containsAll(allRuleEvaluationPoints));
  }
}
