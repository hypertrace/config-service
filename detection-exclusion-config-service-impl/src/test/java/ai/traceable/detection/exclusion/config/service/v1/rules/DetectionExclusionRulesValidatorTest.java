package ai.traceable.detection.exclusion.config.service.v1.rules;

import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_ALERT;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_ALLOW;
import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_BLOCK;
import static ai.traceable.detection.exclusion.config.service.v1.RuleIntent.RULE_INTENT_INTERNAL_TO_EXTERNAL;
import static ai.traceable.detection.exclusion.config.service.v1.RuleSource.RULE_SOURCE_TRACEABLE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

import ai.traceable.detection.exclusion.config.service.v1.CreateDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DeleteDetectionExclusionRuleRequest;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope;
import ai.traceable.detection.exclusion.config.service.v1.EventCondition;
import ai.traceable.detection.exclusion.config.service.v1.GetDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.IpAbuseVelocity;
import ai.traceable.detection.exclusion.config.service.v1.IpAbuseVelocityCondition;
import ai.traceable.detection.exclusion.config.service.v1.RegionCondition;
import ai.traceable.detection.exclusion.config.service.v1.RuleEvaluationPoint;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily;
import ai.traceable.detection.exclusion.config.service.v1.UpdateDetectionExclusionRuleRequest;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DetectionExclusionRulesValidatorTest {

  private DetectionExclusionRulesValidator detectionExclusionRulesValidator;
  private static final String TENANT_ID = "tenantId";
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId(TENANT_ID);

  @BeforeEach
  void setUp() {
    DetectionExclusionConditionValidator conditionValidator =
        mock(DetectionExclusionConditionValidator.class);
    doNothing().when(conditionValidator).validateRuleCondition(any(), any());
    detectionExclusionRulesValidator = new DetectionExclusionRulesValidator(conditionValidator);
  }

  @Test
  void testValidateGetDetectionExclusionRulesRequest() {
    // missing tenant id
    {
      RequestContext requestContext = new RequestContext();
      GetDetectionExclusionRulesRequest request =
          GetDetectionExclusionRulesRequest.newBuilder().build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () -> detectionExclusionRulesValidator.validateOrThrow(requestContext, request));
      assertTrue(throwable.getMessage().contains("Missing expected Tenant ID"));
    }

    // valid request
    {
      RequestContext requestContext = RequestContext.forTenantId("tenantId");
      GetDetectionExclusionRulesRequest request =
          GetDetectionExclusionRulesRequest.newBuilder().build();
      assertDoesNotThrow(
          () -> detectionExclusionRulesValidator.validateOrThrow(requestContext, request));
    }
  }

  @Test
  void testValidateDeleteDetectionExclusionRulesRequest() {
    RequestContext requestContext = RequestContext.forTenantId("tenantId");
    // missing rule id
    {
      DeleteDetectionExclusionRuleRequest request =
          DeleteDetectionExclusionRuleRequest.newBuilder().build();
      assertThrows(
          StatusRuntimeException.class,
          () -> detectionExclusionRulesValidator.validateOrThrow(requestContext, request));
    }

    // valid request
    {
      DeleteDetectionExclusionRuleRequest request =
          DeleteDetectionExclusionRuleRequest.newBuilder().setId("ruleId").build();
      assertDoesNotThrow(
          () -> detectionExclusionRulesValidator.validateOrThrow(requestContext, request));
    }
  }

  @Test
  void testValidateCreateDetectionExclusionRulesRequest() {
    RequestContext requestContext = RequestContext.forTenantId("tenantId");
    // empty condition list
    {
      CreateDetectionExclusionRuleRequest request =
          CreateDetectionExclusionRuleRequest.newBuilder()
              .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("rule"))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  detectionExclusionRulesValidator.validateOrThrow(
                      requestContext, request, List.of()));
      assertTrue(
          throwable.getMessage().contains("DetectionExclusionRule has no specified conditions"));
    }

    // empty env Id string for env scope
    {
      CreateDetectionExclusionRuleRequest request =
          CreateDetectionExclusionRuleRequest.newBuilder()
              .setRuleInfo(
                  DetectionExclusionRuleInfo.newBuilder()
                      .setName("rule")
                      .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                      .addConditions(getDefaultEventCondition()))
              .setRuleScope(
                  DetectionExclusionRuleScope.newBuilder()
                      .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("")))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  detectionExclusionRulesValidator.validateOrThrow(
                      requestContext, request, List.of()));
      assertTrue(throwable.getMessage().contains("Environment id should not be an empty string"));
    }

    // rule name already exists
    {
      CreateDetectionExclusionRuleRequest request =
          CreateDetectionExclusionRuleRequest.newBuilder()
              .setRuleInfo(
                  DetectionExclusionRuleInfo.newBuilder()
                      .setName("ruleName")
                      .addConditions(getDefaultEventCondition()))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  detectionExclusionRulesValidator.validateOrThrow(
                      requestContext,
                      request,
                      List.of(
                          DetectionExclusionRule.newBuilder()
                              .setRuleInfo(
                                  DetectionExclusionRuleInfo.newBuilder().setName("ruleName"))
                              .build())));
      assertTrue(throwable.getMessage().contains("Rule with name ruleName already exists"));
    }

    // valid request
    {
      CreateDetectionExclusionRuleRequest request =
          CreateDetectionExclusionRuleRequest.newBuilder()
              .setRuleInfo(
                  DetectionExclusionRuleInfo.newBuilder()
                      .setName("ruleName1")
                      .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                      .addConditions(getDefaultEventCondition()))
              .build();
      assertDoesNotThrow(
          () ->
              detectionExclusionRulesValidator.validateOrThrow(
                  requestContext,
                  request,
                  List.of(
                      DetectionExclusionRule.newBuilder()
                          .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("ruleName"))
                          .build())));
    }
  }

  @Test
  void testValidateUpdateDetectionExclusionRulesRequest() {
    RequestContext requestContext = RequestContext.forTenantId("tenantId");
    // Rule name already exists
    {
      UpdateDetectionExclusionRuleRequest request =
          UpdateDetectionExclusionRuleRequest.newBuilder()
              .setRule(
                  DetectionExclusionRule.newBuilder()
                      .setId("rule")
                      .setRuleInfo(
                          DetectionExclusionRuleInfo.newBuilder()
                              .setName("ruleName")
                              .addConditions(getDefaultEventCondition())
                              .addRuleEvaluationPoints(
                                  RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                              .setRuleStatus(
                                  DetectionExclusionRuleStatus.newBuilder()
                                      .setRuleCreationSource(RuleSource.RULE_SOURCE_UNSPECIFIED))))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  detectionExclusionRulesValidator.validateOrThrow(
                      requestContext,
                      request,
                      List.of(
                          DetectionExclusionRule.newBuilder()
                              .setRuleInfo(
                                  DetectionExclusionRuleInfo.newBuilder().setName("ruleName"))
                              .build())));
      assertTrue(throwable.getMessage().contains("Rule with name ruleName already exists"));
    }
    // throws INVALID_ARGUMENT if a rule creation source is specified.
    {
      UpdateDetectionExclusionRuleRequest request =
          UpdateDetectionExclusionRuleRequest.newBuilder()
              .setRule(
                  DetectionExclusionRule.newBuilder()
                      .setId("id-1")
                      .setRuleInfo(
                          DetectionExclusionRuleInfo.newBuilder()
                              .setName("test-1")
                              .addRuleEvaluationPoints(
                                  RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                              .setRuleStatus(
                                  DetectionExclusionRuleStatus.newBuilder()
                                      .setRuleCreationSource(RuleSource.RULE_SOURCE_DEFAULT)
                                      .setDisabled(true)
                                      .setHidden(true)
                                      .setGenerateInternalEvents(false))))
              .build();
      assertThrows(
          StatusRuntimeException.class,
          () ->
              detectionExclusionRulesValidator.validateOrThrow(requestContext, request, List.of()));
    }

    // valid request
    {
      UpdateDetectionExclusionRuleRequest request =
          UpdateDetectionExclusionRuleRequest.newBuilder()
              .setRule(
                  DetectionExclusionRule.newBuilder()
                      .setId("rule")
                      .setRuleInfo(
                          DetectionExclusionRuleInfo.newBuilder()
                              .setName("ruleName1")
                              .addConditions(getDefaultEventCondition())
                              .addRuleEvaluationPoints(
                                  RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                              .setRuleStatus(
                                  DetectionExclusionRuleStatus.newBuilder()
                                      .setRuleCreationSource(RuleSource.RULE_SOURCE_UNSPECIFIED))))
              .build();
      assertDoesNotThrow(
          () ->
              detectionExclusionRulesValidator.validateOrThrow(
                  requestContext,
                  request,
                  List.of(
                      DetectionExclusionRule.newBuilder()
                          .setRuleInfo(DetectionExclusionRuleInfo.newBuilder().setName("ruleName"))
                          .build())));
    }
  }

  @Test
  void testCreateDetectionExclusionRuleRequestWithRuleEvaluationPoints() {
    DetectionExclusionRuleScope detectionExclusionRuleScope =
        DetectionExclusionRuleScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-id"))
            .build();
    DetectionExclusionRuleStatus detectionExclusionRuleStatus =
        DetectionExclusionRuleStatus.newBuilder()
            .setRuleCreationSource(RULE_SOURCE_TRACEABLE)
            .setRuleIntent(RULE_INTENT_INTERNAL_TO_EXTERNAL)
            .build();
    RegionCondition regionCondition =
        RegionCondition.newBuilder()
            .addRegions(RegionCondition.Region.newBuilder().setCountryIsoCode("iso"))
            .build();

    // check for empty rule evaluation points
    CreateDetectionExclusionRuleRequest createDetectionExclusionRuleRequest1 =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleScope(detectionExclusionRuleScope)
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("name-1")
                    .setDescription("description-1")
                    .setRuleStatus(detectionExclusionRuleStatus)
                    .addExclusionTargets(EXCLUSION_TARGET_BLOCK)
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setRegionCondition(regionCondition)))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            detectionExclusionRulesValidator.validateOrThrow(
                REQUEST_CONTEXT, createDetectionExclusionRuleRequest1, List.of()));

    // check for empty exclusion targets
    CreateDetectionExclusionRuleRequest createDetectionExclusionRuleRequest2 =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleScope(detectionExclusionRuleScope)
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("name-2")
                    .setDescription("description-2")
                    .setRuleStatus(detectionExclusionRuleStatus)
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setRegionCondition(regionCondition)))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            detectionExclusionRulesValidator.validateOrThrow(
                REQUEST_CONTEXT, createDetectionExclusionRuleRequest2, List.of()));

    // check for empty conditions
    CreateDetectionExclusionRuleRequest createDetectionExclusionRuleRequest3 =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleScope(detectionExclusionRuleScope)
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("name-3")
                    .setDescription("description-3")
                    .setRuleStatus(detectionExclusionRuleStatus)
                    .addExclusionTargets(EXCLUSION_TARGET_BLOCK))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            detectionExclusionRulesValidator.validateOrThrow(
                REQUEST_CONTEXT, createDetectionExclusionRuleRequest3, List.of()));

    CreateDetectionExclusionRuleRequest createDetectionExclusionRuleRequest4 =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleScope(detectionExclusionRuleScope)
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("name-4")
                    .setDescription("description-4")
                    .setRuleStatus(detectionExclusionRuleStatus)
                    .addExclusionTargets(EXCLUSION_TARGET_ALERT)
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setIpAbuseVelocityCondition(
                                IpAbuseVelocityCondition.newBuilder()
                                    .setMaxIpAbuseVelocity(
                                        IpAbuseVelocity.IP_ABUSE_VELOCITY_MEDIUM)))
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            detectionExclusionRulesValidator.validateOrThrow(
                REQUEST_CONTEXT, createDetectionExclusionRuleRequest4, List.of()));

    CreateDetectionExclusionRuleRequest createDetectionExclusionRuleRequest5 =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleScope(detectionExclusionRuleScope)
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("name-5")
                    .setDescription("description-5")
                    .setRuleStatus(detectionExclusionRuleStatus)
                    .addExclusionTargets(EXCLUSION_TARGET_BLOCK)
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setIpAbuseVelocityCondition(
                                IpAbuseVelocityCondition.newBuilder()
                                    .setMaxIpAbuseVelocity(
                                        IpAbuseVelocity.IP_ABUSE_VELOCITY_MEDIUM)))
                    .addRuleEvaluationPoints(
                        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            detectionExclusionRulesValidator.validateOrThrow(
                REQUEST_CONTEXT, createDetectionExclusionRuleRequest5, List.of()));

    // valid case
    CreateDetectionExclusionRuleRequest createDetectionExclusionRuleRequest6 =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleScope(detectionExclusionRuleScope)
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("name-6")
                    .setDescription("description-6")
                    .setRuleStatus(detectionExclusionRuleStatus)
                    .addExclusionTargets(EXCLUSION_TARGET_ALLOW)
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setRegionCondition(regionCondition))
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                    .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                    .addRuleEvaluationPoints(
                        RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
            .build();
    assertDoesNotThrow(
        () ->
            detectionExclusionRulesValidator.validateOrThrow(
                REQUEST_CONTEXT, createDetectionExclusionRuleRequest6, List.of()));
  }

  @Test
  void testUpdateDetectionExclusionRuleRequestWithRuleEvaluationPoints() {
    DetectionExclusionRuleScope detectionExclusionRuleScope =
        DetectionExclusionRuleScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env-id"))
            .build();
    DetectionExclusionRuleStatus detectionExclusionRuleStatus =
        DetectionExclusionRuleStatus.newBuilder()
            .setRuleIntent(RULE_INTENT_INTERNAL_TO_EXTERNAL)
            .build();
    RegionCondition regionCondition =
        RegionCondition.newBuilder()
            .addRegions(RegionCondition.Region.newBuilder().setCountryIsoCode("iso"))
            .build();

    // check for empty rule evaluation points
    UpdateDetectionExclusionRuleRequest updateDetectionExclusionRuleRequest1 =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder()
                            .setRuleStatus(detectionExclusionRuleStatus)
                            .addExclusionTargets(EXCLUSION_TARGET_ALLOW)
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setRegionCondition(regionCondition))
                            .setName("name-1")
                            .setDescription("description-1"))
                    .setRuleScope(detectionExclusionRuleScope)
                    .setId("rule-id-1"))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            detectionExclusionRulesValidator.validateOrThrow(
                REQUEST_CONTEXT, updateDetectionExclusionRuleRequest1, List.of()));

    // check for empty exclusion targets
    UpdateDetectionExclusionRuleRequest updateDetectionExclusionRuleRequest2 =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder()
                            .setRuleStatus(detectionExclusionRuleStatus)
                            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setRegionCondition(regionCondition))
                            .setName("name-2")
                            .setDescription("description-2"))
                    .setRuleScope(detectionExclusionRuleScope)
                    .setId("rule-id-2"))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            detectionExclusionRulesValidator.validateOrThrow(
                REQUEST_CONTEXT, updateDetectionExclusionRuleRequest2, List.of()));

    // check for empty conditions
    UpdateDetectionExclusionRuleRequest updateDetectionExclusionRuleRequest3 =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder()
                            .setRuleStatus(detectionExclusionRuleStatus)
                            .addExclusionTargets(EXCLUSION_TARGET_ALLOW)
                            .addRuleEvaluationPoints(
                                RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT)
                            .setName("name-3")
                            .setDescription("description-3"))
                    .setRuleScope(detectionExclusionRuleScope)
                    .setId("rule-id-3"))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            detectionExclusionRulesValidator.validateOrThrow(
                REQUEST_CONTEXT, updateDetectionExclusionRuleRequest3, List.of()));

    // check for incorrect rule evaluation point (EDGE) - both condition and target unsupported
    UpdateDetectionExclusionRuleRequest updateDetectionExclusionRuleRequest4 =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder()
                            .setRuleStatus(detectionExclusionRuleStatus)
                            .addExclusionTargets(EXCLUSION_TARGET_ALERT)
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setIpAbuseVelocityCondition(
                                        IpAbuseVelocityCondition.newBuilder()
                                            .setMaxIpAbuseVelocity(
                                                IpAbuseVelocity.IP_ABUSE_VELOCITY_MEDIUM)))
                            .setName("name-4")
                            .setDescription("description-4")
                            .addRuleEvaluationPoints(
                                RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE))
                    .setRuleScope(detectionExclusionRuleScope)
                    .setId("rule-id-4"))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            detectionExclusionRulesValidator.validateOrThrow(
                REQUEST_CONTEXT, updateDetectionExclusionRuleRequest4, List.of()));

    // check for incorrect rule evaluation point (INLINE_TRACING_AGENT)
    UpdateDetectionExclusionRuleRequest updateDetectionExclusionRuleRequest5 =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder()
                            .setRuleStatus(detectionExclusionRuleStatus)
                            .addExclusionTargets(EXCLUSION_TARGET_BLOCK)
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setIpAbuseVelocityCondition(
                                        IpAbuseVelocityCondition.newBuilder()
                                            .setMaxIpAbuseVelocity(
                                                IpAbuseVelocity.IP_ABUSE_VELOCITY_MEDIUM)))
                            .setName("name-5")
                            .setDescription("description-5")
                            .addRuleEvaluationPoints(
                                RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
                    .setRuleScope(detectionExclusionRuleScope)
                    .setId("rule-id-5"))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            detectionExclusionRulesValidator.validateOrThrow(
                REQUEST_CONTEXT, updateDetectionExclusionRuleRequest5, List.of()));

    // valid case
    UpdateDetectionExclusionRuleRequest updateDetectionExclusionRuleRequest6 =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder()
                            .setRuleStatus(detectionExclusionRuleStatus)
                            .addExclusionTargets(EXCLUSION_TARGET_ALLOW)
                            .addConditions(
                                DetectionExclusionCondition.newBuilder()
                                    .setRegionCondition(regionCondition))
                            .setName("name-6")
                            .setDescription("description-6")
                            .addRuleEvaluationPoints(
                                RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
                            .addRuleEvaluationPoints(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
                            .addRuleEvaluationPoints(
                                RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT))
                    .setRuleScope(detectionExclusionRuleScope)
                    .setId("rule-id-6"))
            .build();
    assertDoesNotThrow(
        () ->
            detectionExclusionRulesValidator.validateOrThrow(
                REQUEST_CONTEXT, updateDetectionExclusionRuleRequest6, List.of()));
  }

  private DetectionExclusionCondition getDefaultEventCondition() {
    return DetectionExclusionCondition.newBuilder()
        .setEventCondition(
            EventCondition.newBuilder()
                .addSystemDefinedEvents(
                    SystemDefinedEvent.newBuilder()
                        .setEventFamily(
                            SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_API_DEF)))
        .build();
  }
}
