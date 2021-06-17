package ai.traceable.anomaly.config.service.exclusion.validators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.exclusion.GetAnomalyExclusionRulesRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.GetRulesFilter;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class GetAnomalyExclusionRequestValidatorTest {
  private AnomalyConfigValidator anomalyConfigValidator =
      Mockito.mock(AnomalyConfigValidator.class);
  private GetAnomalyExclusionRequestValidator validator;

  @BeforeEach
  void setUp() {
    anomalyConfigValidator = Mockito.mock(AnomalyConfigValidator.class);
    validator = new GetAnomalyExclusionRequestValidator(anomalyConfigValidator);
  }

  @Test
  void testValidator_validRequest() {
    GetAnomalyExclusionRulesRequest validRequest =
        GetAnomalyExclusionRulesRequest.newBuilder()
            .setFilter(
                GetRulesFilter.newBuilder()
                    .addRuleIds("rule_id")
                    .addAnomalyActorIds("actor_id")
                    .addEventTypeIds("event_id")
                    .addEventFamilies(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION)
                    .setAnomalyConfigScope(
                        AnomalyConfigScope.newBuilder()
                            .setApiScope(AnomalyApiScope.newBuilder().setId("id"))))
            .build();
    when(anomalyConfigValidator.validate(any(AnomalyConfigScope.class))).thenReturn(Status.OK);
    assertEquals(Status.OK, validator.validate(validRequest));
  }

  @Test
  void testValidator_invalidRequest() {
    GetAnomalyExclusionRulesRequest invalidRequest =
        GetAnomalyExclusionRulesRequest.newBuilder()
            .setFilter(GetRulesFilter.newBuilder().addRuleIds(""))
            .build();
    when(anomalyConfigValidator.validate(any(AnomalyConfigScope.class))).thenReturn(Status.OK);
    Status status = validator.validate(invalidRequest);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Rule Id can't be empty", status.getDescription());

    invalidRequest =
        GetAnomalyExclusionRulesRequest.newBuilder()
            .setFilter(
                GetRulesFilter.newBuilder()
                    .addEventFamilies(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_UNSPECIFIED))
            .build();
    when(anomalyConfigValidator.validate(any(AnomalyConfigScope.class))).thenReturn(Status.OK);
    status = validator.validate(invalidRequest);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Anomaly Event family not set", status.getDescription());

    invalidRequest =
        GetAnomalyExclusionRulesRequest.newBuilder()
            .setFilter(
                GetRulesFilter.newBuilder()
                    .setAnomalyConfigScope(
                        AnomalyConfigScope.newBuilder()
                            .setApiScope(AnomalyApiScope.newBuilder().setId("id").build())))
            .build();
    when(anomalyConfigValidator.validate(invalidRequest.getFilter().getAnomalyConfigScope()))
        .thenReturn(Status.INVALID_ARGUMENT.withDescription("a"));
    status = validator.validate(invalidRequest);
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("a", status.getDescription());
  }
}
