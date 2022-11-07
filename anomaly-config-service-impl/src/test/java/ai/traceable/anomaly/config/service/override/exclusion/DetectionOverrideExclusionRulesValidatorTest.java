package ai.traceable.anomaly.config.service.override.exclusion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.override.common.validator.DetectionOverrideConfigsValidator;
import ai.traceable.anomaly.config.service.override.common.validator.DetectionOverrideEventsValidator;
import ai.traceable.anomaly.config.service.override.common.validator.DetectionOverrideScopeValidator;
import ai.traceable.anomaly.config.service.v1.override.CreateDetectionExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.override.DeleteDetectionExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.override.DetectionExclusionConfigCriteria;
import ai.traceable.anomaly.config.service.v1.override.DetectionExclusionRule;
import ai.traceable.anomaly.config.service.v1.override.DetectionExclusionRuleInfo;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideRuleScope;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideRuleStatus;
import ai.traceable.anomaly.config.service.v1.override.DetectionOverrideTargetEventsConfig;
import ai.traceable.anomaly.config.service.v1.override.UpdateDetectionExclusionRuleRequest;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DetectionOverrideExclusionRulesValidatorTest {

  private DetectionOverrideEventsValidator eventsValidator;
  private DetectionOverrideScopeValidator scopeValidator;
  private DetectionOverrideExclusionRulesValidator rulesValidator;

  @BeforeEach
  void setup() {
    this.eventsValidator = mock(DetectionOverrideEventsValidator.class);
    this.scopeValidator = mock(DetectionOverrideScopeValidator.class);
    this.rulesValidator =
        new DetectionOverrideExclusionRulesValidator(
            mock(DetectionOverrideConfigsValidator.class), eventsValidator, scopeValidator);
  }

  @Test
  public void testValidateCreateRequest() {
    CreateDetectionExclusionRuleRequest request =
        CreateDetectionExclusionRuleRequest.getDefaultInstance();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), rulesValidator.validate(request).getCode());

    request =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleScope(DetectionOverrideRuleScope.getDefaultInstance())
            .build();

    Status status = rulesValidator.validate(request);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Create detection exclusion rule request should have a valid rule info / rule scope.",
        status.getDescription());

    request =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setRuleScope(DetectionOverrideRuleScope.getDefaultInstance())
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("test-name")
                    .setCriteria(
                        DetectionExclusionConfigCriteria.newBuilder()
                            .setTargetEventsConfig(
                                DetectionOverrideTargetEventsConfig.getDefaultInstance())
                            .build())
                    .build())
            .build();

    when(eventsValidator.validateTargetEventsConfig(any())).thenReturn(Status.OK);
    when(scopeValidator.validateRuleScope(any())).thenReturn(Status.OK);
    assertEquals(Status.OK, rulesValidator.validate(request));
  }

  @Test
  public void testValidateUpdateRequest() {
    UpdateDetectionExclusionRuleRequest request =
        UpdateDetectionExclusionRuleRequest.getDefaultInstance();
    Status status = rulesValidator.validate(request);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        "Update detection exclusion rule request should have a valid rule.",
        status.getDescription());

    request =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(DetectionExclusionRule.newBuilder().setId("").build())
            .build();

    status = rulesValidator.validate(request);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Detection rule shouldn't have an empty id.", status.getDescription());

    request =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setId("test-id")
                    .setRuleScope(DetectionOverrideRuleScope.getDefaultInstance())
                    .build())
            .build();

    status = rulesValidator.validate(request);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Detection rule should have a valid scope/info/status.", status.getDescription());

    request =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setId("test-id")
                    .setRuleInfo(DetectionExclusionRuleInfo.getDefaultInstance())
                    .setRuleScope(DetectionOverrideRuleScope.getDefaultInstance())
                    .setRuleStatus(DetectionOverrideRuleStatus.getDefaultInstance())
                    .build())
            .build();

    when(scopeValidator.validateRuleScope(any())).thenReturn(Status.OK);
    status = rulesValidator.validate(request);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Rule info shouldn't have an empty name.", status.getDescription());

    request =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setId("test-id")
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder().setName("test-name").build())
                    .setRuleScope(DetectionOverrideRuleScope.getDefaultInstance())
                    .setRuleStatus(DetectionOverrideRuleStatus.getDefaultInstance())
                    .build())
            .build();

    when(scopeValidator.validateRuleScope(any())).thenReturn(Status.OK);
    status = rulesValidator.validate(request);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Rule info should have a valid criteria.", status.getDescription());

    request =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setId("test-id")
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder()
                            .setName("test-name")
                            .setCriteria(DetectionExclusionConfigCriteria.getDefaultInstance())
                            .build())
                    .setRuleScope(DetectionOverrideRuleScope.getDefaultInstance())
                    .setRuleStatus(DetectionOverrideRuleStatus.getDefaultInstance())
                    .build())
            .build();

    when(scopeValidator.validateRuleScope(any())).thenReturn(Status.OK);
    status = rulesValidator.validate(request);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Criteria should have a valid target events config.", status.getDescription());

    request =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setId("test-id")
                    .setRuleInfo(
                        DetectionExclusionRuleInfo.newBuilder()
                            .setName("test-name")
                            .setCriteria(
                                DetectionExclusionConfigCriteria.newBuilder()
                                    .setTargetEventsConfig(
                                        DetectionOverrideTargetEventsConfig.getDefaultInstance())
                                    .build())
                            .build())
                    .setRuleScope(DetectionOverrideRuleScope.getDefaultInstance())
                    .setRuleStatus(DetectionOverrideRuleStatus.getDefaultInstance())
                    .build())
            .build();

    when(scopeValidator.validateRuleScope(any())).thenReturn(Status.OK);
    when(eventsValidator.validateTargetEventsConfig(any())).thenReturn(Status.OK);
    assertEquals(Status.OK, rulesValidator.validate(request));
  }

  @Test
  public void testValidateDeleteRequest() {
    assertEquals(
        Status.OK,
        rulesValidator.validate(
            DeleteDetectionExclusionRuleRequest.newBuilder().setRuleId("id").build()));

    Status status =
        rulesValidator.validate(
            DeleteDetectionExclusionRuleRequest.newBuilder().setRuleId("").build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
  }
}
