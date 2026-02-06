package ai.traceable.config.service.migration;

import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_BLOCK;
import static ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT;
import static ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClientConfig;
import ai.traceable.detection.exclusion.config.service.v1.CreateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceGrpc;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.EventCondition;
import ai.traceable.detection.exclusion.config.service.v1.GetDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.IpAddressCondition;
import ai.traceable.detection.exclusion.config.service.v1.IpAddressConditionType;
import ai.traceable.detection.exclusion.config.service.v1.RegionCondition;
import ai.traceable.detection.exclusion.config.service.v1.RuleChangeSource;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily;
import ai.traceable.detection.exclusion.config.service.v1.UpdateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionAuditHelper;
import ai.traceable.detection.exclusion.config.service.v1.rules.DetectionExclusionRulesStore;
import com.google.protobuf.Value;
import com.typesafe.config.ConfigFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DetectionExclusionRuleEvaluationPointsMigrationConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static DetectionExclusionConfigServiceGrpc.DetectionExclusionConfigServiceBlockingStub
      detectionExclusionConfigServiceBlockingStub;
  private static DetectionExclusionRulesStore detectionExclusionRulesStore;

  private static final String TENANT_ID = "tenant1";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);
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
  void testMigrationForCreateAndUpdateRequestsForRuleEvaluationPoints() {
    DetectionExclusionRuleStatus detectionExclusionRuleStatus =
        DetectionExclusionRuleStatus.newBuilder()
            .setChangeSource(RuleChangeSource.RULE_CHANGE_SOURCE_TRACEABLE)
            .build();

    DetectionExclusionRuleInfo edgeRuleInfoWithoutEvaluationPoints =
        DetectionExclusionRuleInfo.newBuilder()
            .setName("detection-exclusion-rule-without-RuleEvaluationPoints")
            .addExclusionTargets(EXCLUSION_TARGET_BLOCK)
            .addConditions(
                DetectionExclusionCondition.newBuilder()
                    .setEventCondition(
                        EventCondition.newBuilder()
                            .addSystemDefinedEvents(
                                SystemDefinedEvent.newBuilder()
                                    .setEventFamily(
                                        SystemDefinedEventFamily
                                            .SYSTEM_DEFINED_EVENT_FAMILY_API_DEF))))
            .setRuleStatus(detectionExclusionRuleStatus)
            .build();

    DetectionExclusionRuleInfo modsecRuleInfoWithoutEvaluationPoints =
        DetectionExclusionRuleInfo.newBuilder()
            .setName("modsec-rule-without-RuleEvaluationPoints")
            .addExclusionTargets(EXCLUSION_TARGET_BLOCK)
            .addConditions(
                DetectionExclusionCondition.newBuilder()
                    .setEventCondition(
                        EventCondition.newBuilder()
                            .addSystemDefinedEvents(
                                SystemDefinedEvent.newBuilder()
                                    .setEventFamily(
                                        SystemDefinedEventFamily
                                            .SYSTEM_DEFINED_EVENT_FAMILY_MODSEC))))
            .setRuleStatus(detectionExclusionRuleStatus)
            .build();

    DetectionExclusionRuleScope detectionExclusionRuleScope1 =
        DetectionExclusionRuleScope.newBuilder()
            .setEnvironmentScope(
                ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope.newBuilder()
                    .addEnvironmentIds("env-id-1"))
            .build();
    DetectionExclusionRuleScope detectionExclusionRuleScope2 =
        DetectionExclusionRuleScope.newBuilder()
            .setEnvironmentScope(
                ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope.newBuilder()
                    .addEnvironmentIds("env-id-2"))
            .build();

    CreateDetectionExclusionRuleRequest createEdgeRuleRequest =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleInfo(edgeRuleInfoWithoutEvaluationPoints)
            .setRuleScope(detectionExclusionRuleScope1)
            .build();

    CreateDetectionExclusionRuleRequest createModsecRuleRequest =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleInfo(modsecRuleInfoWithoutEvaluationPoints)
            .setRuleScope(detectionExclusionRuleScope1)
            .build();

    DetectionExclusionRule edgeRule =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                detectionExclusionConfigServiceBlockingStub
                    .createDetectionExclusionRule(createEdgeRuleRequest)
                    .getRule());
    DetectionExclusionRule modsecRule =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                detectionExclusionConfigServiceBlockingStub
                    .createDetectionExclusionRule(createModsecRuleRequest)
                    .getRule());

    // migration should have happened for the rule-creation requests above
    assertTrue(
        edgeRule
            .getRuleInfo()
            .getRuleEvaluationPointsList()
            .containsAll(
                Arrays.asList(
                    RULE_EVALUATION_POINT_PLATFORM, RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)));
    assertTrue(
        modsecRule
            .getRuleInfo()
            .getRuleEvaluationPointsList()
            .containsAll(
                Arrays.asList(
                    RULE_EVALUATION_POINT_PLATFORM, RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)));

    DetectionExclusionRule ruleToBeUpdated =
        DetectionExclusionRule.newBuilder()
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("updated-rule-name")
                    .setDescription("updated-rule-description")
                    .addExclusionTargets(EXCLUSION_TARGET_BLOCK)
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setRegionCondition(
                                RegionCondition.newBuilder()
                                    .addRegions(
                                        RegionCondition.Region.newBuilder()
                                            .setCountryIsoCode("iso")))))
            .setRuleScope(detectionExclusionRuleScope2)
            .setId(edgeRule.getId())
            .build();

    DetectionExclusionRule updatedRule =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    detectionExclusionConfigServiceBlockingStub.updateDetectionExclusionRule(
                        UpdateDetectionExclusionRuleRequest.newBuilder()
                            .setRule(ruleToBeUpdated)
                            .build()))
            .getRule();

    // migration should have happened for the update request above
    assertTrue(
        updatedRule
            .getRuleInfo()
            .getRuleEvaluationPointsList()
            .containsAll(
                Arrays.asList(
                    RULE_EVALUATION_POINT_PLATFORM, RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)));
  }

  @Test
  void testMigrationForGetRulesRequestForRuleEvaluationPoints() {
    /*
     * Workflow for getDetectionExclusionRules - this will migrate all rules without RuleEvaluationPoints
     */
    upsertDetectionExclusionRuleWithoutRuleEvaluationPoints();
    List<DetectionExclusionRule> fetchedRules = fetchAllDetectionExclusionRules();

    // Default 17 + 1 rule without any rule evaluation points
    assertEquals(19, fetchedRules.size());

    /*
     * Now, its expected that all the fetched rules will have a non-empty list of RuleEvaluationPoints
     */
    fetchedRules.forEach(
        detectionExclusionRule ->
            assertFalse(
                detectionExclusionRule.getRuleInfo().getRuleEvaluationPointsList().isEmpty()));
  }

  private void upsertDetectionExclusionRuleWithoutRuleEvaluationPoints() {
    String detectionExclusionRuleId = UUID.randomUUID().toString();
    DetectionExclusionRule detectionExclusionRule =
        DetectionExclusionRule.newBuilder()
            .setId(detectionExclusionRuleId)
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("detection-exclusion-rule-with-no-rule-evaluation-points")
                    .setDescription("description")
                    .addExclusionTargets(EXCLUSION_TARGET_BLOCK)
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setIpAddressCondition(
                                IpAddressCondition.newBuilder()
                                    .setIpAddressConditionType(
                                        IpAddressConditionType
                                            .IP_ADDRESS_CONDITION_TYPE_ALL_INTERNAL)
                                    .addIpAddresses("1.2.3.4")))
                    .clearRuleEvaluationPoints())
            .setRuleScope(
                DetectionExclusionRuleScope.newBuilder()
                    .setEnvironmentScope(
                        ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope
                            .newBuilder()
                            .addEnvironmentIds("env-id")))
            .build();

    detectionExclusionRulesStore.upsertObjects(REQUEST_CONTEXT, List.of(detectionExclusionRule));
  }

  private List<DetectionExclusionRule> fetchAllDetectionExclusionRules() {
    return GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            detectionExclusionConfigServiceBlockingStub
                .getDetectionExclusionRules(GetDetectionExclusionRulesRequest.getDefaultInstance())
                .getRulesList());
  }
}
