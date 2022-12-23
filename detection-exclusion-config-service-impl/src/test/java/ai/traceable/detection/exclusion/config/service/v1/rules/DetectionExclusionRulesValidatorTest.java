package ai.traceable.detection.exclusion.config.service.v1.rules;

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
import ai.traceable.detection.exclusion.config.service.v1.GetDetectionExclusionRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.RuleChangeSource;
import ai.traceable.detection.exclusion.config.service.v1.UpdateDetectionExclusionRuleRequest;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DetectionExclusionRulesValidatorTest {

  private DetectionExclusionConditionValidator conditionValidator;
  private DetectionExclusionRulesValidator detectionExclusionRulesValidator;

  @BeforeEach
  void setUp() {
    conditionValidator = mock(DetectionExclusionConditionValidator.class);
    doNothing().when(conditionValidator).validateRuleCondition(any());
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
                      .addConditions(DetectionExclusionCondition.getDefaultInstance())
                      .setRuleStatus(
                          DetectionExclusionRuleStatus.newBuilder()
                              .setChangeSource(RuleChangeSource.RULE_CHANGE_SOURCE_CUSTOMER)))
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
                      .addConditions(DetectionExclusionCondition.getDefaultInstance())
                      .setRuleStatus(
                          DetectionExclusionRuleStatus.newBuilder()
                              .setChangeSource(RuleChangeSource.RULE_CHANGE_SOURCE_CUSTOMER)))
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
                      .addConditions(DetectionExclusionCondition.getDefaultInstance())
                      .setRuleStatus(
                          DetectionExclusionRuleStatus.newBuilder()
                              .setChangeSource(RuleChangeSource.RULE_CHANGE_SOURCE_CUSTOMER)))
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
    // rule change source not set
    {
      UpdateDetectionExclusionRuleRequest request =
          UpdateDetectionExclusionRuleRequest.newBuilder()
              .setRule(
                  DetectionExclusionRule.newBuilder()
                      .setId("id")
                      .setRuleInfo(
                          DetectionExclusionRuleInfo.newBuilder()
                              .setName("rule")
                              .addConditions(DetectionExclusionCondition.getDefaultInstance())))
              .build();
      Throwable throwable =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  detectionExclusionRulesValidator.validateOrThrow(
                      requestContext, request, List.of()));
      assertTrue(throwable.getMessage().contains("Invalid ruleChangeSource"));
    }

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
                              .addConditions(DetectionExclusionCondition.getDefaultInstance())
                              .setRuleStatus(
                                  DetectionExclusionRuleStatus.newBuilder()
                                      .setChangeSource(
                                          RuleChangeSource.RULE_CHANGE_SOURCE_CUSTOMER))))
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
      UpdateDetectionExclusionRuleRequest request =
          UpdateDetectionExclusionRuleRequest.newBuilder()
              .setRule(
                  DetectionExclusionRule.newBuilder()
                      .setId("rule")
                      .setRuleInfo(
                          DetectionExclusionRuleInfo.newBuilder()
                              .setName("ruleName1")
                              .addConditions(DetectionExclusionCondition.getDefaultInstance())
                              .setRuleStatus(
                                  DetectionExclusionRuleStatus.newBuilder()
                                      .setChangeSource(
                                          RuleChangeSource.RULE_CHANGE_SOURCE_CUSTOMER))))
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
}
