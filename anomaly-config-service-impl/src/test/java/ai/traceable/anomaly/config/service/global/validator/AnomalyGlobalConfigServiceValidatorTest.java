package ai.traceable.anomaly.config.service.global.validator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ai.traceable.anomaly.config.service.common.AnomalyConfigValidator;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.RuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersionType;
import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersionsFilter;
import ai.traceable.anomaly.config.service.v1.global.DeleteScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAnomalyRuleInfosRequest;
import ai.traceable.anomaly.config.service.v1.global.GetAvailableRuleVersionsRequest;
import ai.traceable.anomaly.config.service.v1.global.GetScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.GetUnresolvedScopedAnomalyGlobalConfigStatusRequest;
import ai.traceable.anomaly.config.service.v1.global.ScopedAnomalyConfigStatusChange;
import ai.traceable.anomaly.config.service.v1.global.UpdateScopedAnomalyGlobalConfigStatusRequest;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class AnomalyGlobalConfigServiceValidatorTest {
  private final AnomalyConfigValidator configValidator = mock(AnomalyConfigValidator.class);
  private final AnomalyConfigScope configScope = AnomalyConfigScope.newBuilder().build();
  private final AnomalyConfigStatusChange configStatusChange =
      AnomalyConfigStatusChange.newBuilder().build();
  private AnomalyGlobalConfigServiceValidator globalValidator;

  @BeforeEach
  void setUp() {
    doReturn(Status.OK).when(configValidator).validate(configScope);
    doReturn(Status.OK).when(configValidator).validate(configStatusChange);
    this.globalValidator = new AnomalyGlobalConfigServiceValidator(configValidator);
  }

  @Nested
  class GlobalConfigStatusValidation {

    @Test
    void testValidateDeleteStatusRequest() {
      Status status =
          globalValidator.validate(
              DeleteScopedAnomalyGlobalConfigStatusRequest.getDefaultInstance());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
      AnomalyConfigScope configScope =
          AnomalyConfigScope.newBuilder()
              .setServiceScope(AnomalyServiceScope.newBuilder().setId("serviceId").build())
              .build();
      doReturn(Status.OK).when(configValidator).validate(configScope);
      status =
          globalValidator.validate(
              DeleteScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                  .setConfigScope(configScope)
                  .build());
      assertEquals(Status.OK.getCode(), status.getCode());
    }

    @Test
    void testValidateGetStatusRequest() {
      Status status =
          globalValidator.validate(GetScopedAnomalyGlobalConfigStatusRequest.getDefaultInstance());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
      assertTrue(status.getDescription().contains("valid config scope"));
      verify(configValidator, times(0)).validate((AnomalyConfigScope) any());

      status =
          globalValidator.validate(
              GetScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                  .setConfigScope(configScope)
                  .build());
      assertEquals(Status.OK.getCode(), status.getCode());
      verify(configValidator, times(1)).validate((AnomalyConfigScope) any());
    }

    @Test
    void testValidateGetUnresolvedStatusRequest() {
      Status status =
          globalValidator.validate(
              GetUnresolvedScopedAnomalyGlobalConfigStatusRequest.getDefaultInstance());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
      assertTrue(status.getDescription().contains("valid config scope"));
      verify(configValidator, times(0)).validate((AnomalyConfigScope) any());

      status =
          globalValidator.validate(
              GetUnresolvedScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                  .setConfigScope(configScope)
                  .build());
      assertEquals(Status.OK.getCode(), status.getCode());
      verify(configValidator, times(1)).validate((AnomalyConfigScope) any());
    }

    @Test
    void testValidateUpdateStatusRequest() {
      Status status =
          globalValidator.validate(
              UpdateScopedAnomalyGlobalConfigStatusRequest.getDefaultInstance());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
      assertTrue(status.getDescription().contains("valid scoped config change object"));
      verify(configValidator, times(0)).validate((AnomalyConfigStatusChange) any());
      verify(configValidator, times(0)).validate((AnomalyConfigScope) any());

      status =
          globalValidator.validate(
              UpdateScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                  .setScopedConfig(
                      ScopedAnomalyConfigStatusChange.newBuilder()
                          .setConfigStatus(configStatusChange))
                  .build());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
      assertTrue(status.getDescription().contains("valid config scope"));
      verify(configValidator, times(0)).validate((AnomalyConfigStatusChange) any());
      verify(configValidator, times(0)).validate((AnomalyConfigScope) any());

      status =
          globalValidator.validate(
              UpdateScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                  .setScopedConfig(
                      ScopedAnomalyConfigStatusChange.newBuilder()
                          .setConfigStatus(configStatusChange)
                          .setConfigScope(configScope))
                  .build());
      assertEquals(Status.OK.getCode(), status.getCode());
      verify(configValidator, times(1)).validate((AnomalyConfigStatusChange) any());
      verify(configValidator, times(1)).validate((AnomalyConfigScope) any());

      clearInvocations(configValidator);
      doReturn(Status.INVALID_ARGUMENT).when(configValidator).validate(configStatusChange);
      status =
          globalValidator.validate(
              UpdateScopedAnomalyGlobalConfigStatusRequest.newBuilder()
                  .setScopedConfig(
                      ScopedAnomalyConfigStatusChange.newBuilder()
                          .setConfigStatus(configStatusChange)
                          .setConfigScope(configScope))
                  .build());
      assertEquals(Status.INVALID_ARGUMENT.getCode(), status.getCode());
      verify(configValidator, times(1)).validate((AnomalyConfigStatusChange) any());
      verify(configValidator, times(1)).validate((AnomalyConfigScope) any());
    }
  }

  @Nested
  class RuleInfoValidation {
    @Test
    @DisplayName("Should return OK status when a given valid get event family request")
    void validate_OK() {
      GetAnomalyRuleInfosRequest request =
          GetAnomalyRuleInfosRequest.newBuilder()
              .addAllEventFamilies(
                  List.of(
                      AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC,
                      AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF))
              .build();
      Status status = globalValidator.validate(request);
      assertEquals(Status.Code.OK, status.getCode());
    }

    @Test
    @DisplayName(
        "Should return INVALID_ARGUMENT status when a given an invalid get event family request")
    void validate_error() {
      GetAnomalyRuleInfosRequest request =
          GetAnomalyRuleInfosRequest.newBuilder()
              .addAllEventFamilies(
                  List.of(
                      AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC,
                      AnomalyEventFamily.ANOMALY_EVENT_FAMILY_UNSPECIFIED))
              .build();
      Status status = globalValidator.validate(request);
      assertEquals(Code.INVALID_ARGUMENT, status.getCode());
    }
  }

  @Test
  void validate_getAvailableRuleVersionsRequest() {
    GetAvailableRuleVersionsRequest validRequest =
        GetAvailableRuleVersionsRequest.newBuilder()
            .setRuleType(RuleType.RULE_TYPE_WEB_APPLICATION)
            .setFilter(
                AvailableRuleVersionsFilter.newBuilder()
                    .addVersionTypes(RuleVersionType.RULE_VERSION_TYPE_STABLE)
                    .build())
            .build();

    Status status = globalValidator.validate(validRequest);
    assertEquals(Status.Code.OK, status.getCode());

    GetAvailableRuleVersionsRequest invalidRequest =
        GetAvailableRuleVersionsRequest.newBuilder().build();
    status = globalValidator.validate(invalidRequest);
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());

    GetAvailableRuleVersionsRequest invalidRequest2 =
        GetAvailableRuleVersionsRequest.newBuilder()
            .setRuleType(RuleType.RULE_TYPE_API_PROTECTION)
            .setFilter(
                AvailableRuleVersionsFilter.newBuilder()
                    .addVersionTypes(RuleVersionType.RULE_VERSION_TYPE_STABLE)
                    .addVersionTypes(RuleVersionType.RULE_VERSION_TYPE_EXPERIMENTAL)
                    .build())
            .build();
    status = globalValidator.validate(invalidRequest2);
    assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
  }
}
