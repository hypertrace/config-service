package ai.traceable.threatscoring.config.service.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.threatscoring.config.service.v1.AnomalousEventConfidenceConfig;
import ai.traceable.threatscoring.config.service.v1.EventConfidenceScoringConfig;
import ai.traceable.threatscoring.config.service.v1.OverrideEventConfidenceScoringConfigRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Objects;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ThreatScoringConfigRequestValidatorTest {
  @Mock private RequestContext mockRequestContext;
  private ThreatScoringConfigRequestValidator validator = new ThreatScoringConfigRequestValidator();
  private static final String TEST_TENANT_ID = "ThreatScoringConfigRequestValidatorTest-tenant-id";

  @Test
  void validatesGetRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID", () -> validator.validateGetOrDeleteRequest(mockRequestContext));
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(() -> validator.validateGetOrDeleteRequest(mockRequestContext));
  }

  @Test
  void validateOverrideEventConfidenceScoringConfigRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            validator.validateOverrideEventConfidenceScoringConfigRequest(
                mockRequestContext,
                OverrideEventConfidenceScoringConfigRequest.newBuilder().build()));
  }

  @Test
  void validateAnomalousEventConfidenceScoringConfig() {
    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "Min number of unique unexpected characters should be > 0",
        () ->
            validator.validateOverrideEventConfidenceScoringConfigRequest(
                mockRequestContext,
                OverrideEventConfidenceScoringConfigRequest.newBuilder()
                    .setEventConfidenceScoringConfig(
                        EventConfidenceScoringConfig.newBuilder()
                            .setAnomalousEventConfidenceConfig(
                                AnomalousEventConfidenceConfig.newBuilder()
                                    .setMinNumberOfUniqueUnexpectedCharacters(-1)
                                    .build())
                            .build())
                    .build()));

    assertInvalidArgStatusContaining(
        "Min number of unlearnt params in api should be > 0",
        () ->
            validator.validateOverrideEventConfidenceScoringConfigRequest(
                mockRequestContext,
                OverrideEventConfidenceScoringConfigRequest.newBuilder()
                    .setEventConfidenceScoringConfig(
                        EventConfidenceScoringConfig.newBuilder()
                            .setAnomalousEventConfidenceConfig(
                                AnomalousEventConfidenceConfig.newBuilder()
                                    .setMinNumberOfUnlearntParamsInApi(-1)
                                    .build())
                            .build())
                    .build()));
  }

  private void assertInvalidArgStatusContaining(String text, Executable executable) {
    StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, executable);
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    assertTrue(
        Objects.requireNonNull(exception.getStatus().getDescription()).contains(text),
        "Expected arg to contain " + text + " but was " + exception.getMessage());
  }
}
