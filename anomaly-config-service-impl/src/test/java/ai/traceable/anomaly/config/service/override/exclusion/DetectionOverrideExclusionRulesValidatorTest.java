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
import ai.traceable.anomaly.config.service.v1.override.DetectionExclusionRuleConfig;
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
            .setConfig(DetectionExclusionRuleConfig.getDefaultInstance())
            .setRuleScope(DetectionOverrideRuleScope.getDefaultInstance())
            .build();

    Status status = rulesValidator.validate(request);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Config should have a valid criteria / rule status.", status.getDescription());

    request =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setConfig(
                DetectionExclusionRuleConfig.newBuilder()
                    .setCriteria(DetectionExclusionConfigCriteria.newBuilder().build())
                    .setRuleStatus(DetectionOverrideRuleStatus.getDefaultInstance())
                    .build())
            .setRuleScope(DetectionOverrideRuleScope.getDefaultInstance())
            .build();

    status = rulesValidator.validate(request);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Criteria should have a valid target events config.", status.getDescription());

    request =
        CreateDetectionExclusionRuleRequest.newBuilder()
            .setConfig(
                DetectionExclusionRuleConfig.newBuilder()
                    .setCriteria(
                        DetectionExclusionConfigCriteria.newBuilder()
                            .setTargetEventsConfig(
                                DetectionOverrideTargetEventsConfig.getDefaultInstance())
                            .build())
                    .setRuleStatus(DetectionOverrideRuleStatus.getDefaultInstance())
                    .build())
            .setRuleScope(DetectionOverrideRuleScope.getDefaultInstance())
            .build();

    when(eventsValidator.validateTargetEventsConfig(any())).thenReturn(Status.OK);
    when(scopeValidator.validateRuleScope(any())).thenReturn(Status.OK);
    assertEquals(Status.OK, rulesValidator.validate(request));
  }

  @Test
  public void testValidateUpdateRequest() {
    UpdateDetectionExclusionRuleRequest request =
        UpdateDetectionExclusionRuleRequest.getDefaultInstance();
    assertEquals(Status.INVALID_ARGUMENT.getCode(), rulesValidator.validate(request).getCode());

    request =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(DetectionExclusionRule.newBuilder().setId("").build())
            .build();

    Status status = rulesValidator.validate(request);
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
    assertEquals("Detection rule should have a valid scope/config.", status.getDescription());

    request =
        UpdateDetectionExclusionRuleRequest.newBuilder()
            .setRule(
                DetectionExclusionRule.newBuilder()
                    .setId("test-id")
                    .setRuleScope(DetectionOverrideRuleScope.getDefaultInstance())
                    .setConfig(
                        DetectionExclusionRuleConfig.newBuilder()
                            .setCriteria(
                                DetectionExclusionConfigCriteria.newBuilder()
                                    .setTargetEventsConfig(
                                        DetectionOverrideTargetEventsConfig.getDefaultInstance())
                                    .build())
                            .setRuleStatus(DetectionOverrideRuleStatus.getDefaultInstance())
                            .build())
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
