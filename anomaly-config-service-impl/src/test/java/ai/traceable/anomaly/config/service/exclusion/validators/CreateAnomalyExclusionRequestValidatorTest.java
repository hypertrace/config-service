package ai.traceable.anomaly.config.service.exclusion.validators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActor;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActorExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;
import ai.traceable.anomaly.config.service.v1.exclusion.CreateAnomalyExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionType;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CreateAnomalyExclusionRequestValidatorTest {
  private AnomalyConfigValidator scopeValidator;
  private CreateAnomalyExclusionRequestValidator validator;

  @BeforeEach
  void setUp() {
    scopeValidator = Mockito.mock(AnomalyConfigValidator.class);
    validator = new CreateAnomalyExclusionRequestValidator(scopeValidator);
  }

  @Test
  void testValidate() {
    AnomalyExclusionRuleData.Builder invalidDataBuilder =
        AnomalyExclusionRuleData.newBuilder().setName("rule_name");
    // missing data
    Status status =
        validator.validate(
            CreateAnomalyExclusionRuleRequest.newBuilder()
                .setRuleData(invalidDataBuilder.build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals(
        String.format(
            "Missing data, unable to create exclusion rule for [%s]", invalidDataBuilder.build()),
        status.getDescription());

    AnomalyConfigScope mockScope =
        AnomalyConfigScope.newBuilder()
            .setApiScope(AnomalyApiScope.newBuilder().setId("id").build())
            .build();
    invalidDataBuilder.setAnomalyConfigScope(mockScope);
    invalidDataBuilder.setEventExclusionInfo(
        EventExclusionInfo.newBuilder().getDefaultInstanceForType());
    invalidDataBuilder.setAnomalyActorExclusionInfo(
        AnomalyActorExclusionInfo.newBuilder().getDefaultInstanceForType());

    // invalid scope
    when(scopeValidator.validate(mockScope))
        .thenReturn(Status.INVALID_ARGUMENT.withDescription("a"));
    status =
        validator.validate(
            CreateAnomalyExclusionRuleRequest.newBuilder()
                .setRuleData(invalidDataBuilder.build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("a", status.getDescription());

    // Invalid event exclusion info
    when(scopeValidator.validate(mockScope)).thenReturn(Status.OK);
    invalidDataBuilder.setEventExclusionInfo(
        EventExclusionInfo.newBuilder()
            .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE)
            .setEventTypeId("event_id")
            .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION)
            .build());
    status =
        validator.validate(
            CreateAnomalyExclusionRuleRequest.newBuilder()
                .setRuleData(invalidDataBuilder.build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("Invalid event exclusion info", status.getDescription());
    invalidDataBuilder.setEventExclusionInfo(
        EventExclusionInfo.newBuilder()
            .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE)
            .setEventTypeId("event_id")
            .setEventTypeName("event_name")
            .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION)
            .build());

    // missing actor i
    invalidDataBuilder.setAnomalyActorExclusionInfo(
        AnomalyActorExclusionInfo.newBuilder()
            .setAnomalyActor(AnomalyActor.newBuilder().getDefaultInstanceForType()));
    status =
        validator.validate(
            CreateAnomalyExclusionRuleRequest.newBuilder()
                .setRuleData(invalidDataBuilder.build())
                .build());
    assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
    assertEquals("missing anomaly actor id", status.getDescription());

    // equivalent to disable rule
    AnomalyConfigScope customerScope =
        AnomalyConfigScope.newBuilder()
            .setCustomerScope(AnomalyCustomerScope.newBuilder().build())
            .build();
    invalidDataBuilder.setAnomalyConfigScope(customerScope);
    when(scopeValidator.validate(customerScope)).thenReturn(Status.OK);
    invalidDataBuilder.setAnomalyActorExclusionInfo(AnomalyActorExclusionInfo.newBuilder().build());
    status =
        validator.validate(
            CreateAnomalyExclusionRuleRequest.newBuilder()
                .setRuleData(invalidDataBuilder.build())
                .build());
    assertEquals(Status.UNIMPLEMENTED.getCode(), status.getCode());
    assertEquals(
        "Excluding all anomaly actors for all APIs is equivalent to disabling detection, operation not supported",
        status.getDescription());

    // making it valid by setting allowed threat actor
    invalidDataBuilder.setAnomalyActorExclusionInfo(
        AnomalyActorExclusionInfo.newBuilder()
            .setAnomalyActor(AnomalyActor.newBuilder().setId("id").build())
            .build());
    status =
        validator.validate(
            CreateAnomalyExclusionRuleRequest.newBuilder()
                .setRuleData(invalidDataBuilder.build())
                .build());
    assertEquals(Status.OK.getCode(), status.getCode());
  }
}
