package ai.traceable.runner.logs.config.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.runner.logs.config.service.v1.AstScanLogsPersistenceConfigUpdate;
import ai.traceable.runner.logs.config.service.v1.AstScanLogsPurgeConfigUpdate;
import ai.traceable.runner.logs.config.service.v1.GetRunnerLogsPersistenceConfigRequest;
import ai.traceable.runner.logs.config.service.v1.GetRunnerLogsPurgeConfigRequest;
import ai.traceable.runner.logs.config.service.v1.RunnerLogsPersistenceConfigUpdate;
import ai.traceable.runner.logs.config.service.v1.RunnerLogsPurgeConfigUpdate;
import ai.traceable.runner.logs.config.service.v1.UpdateRunnerLogsPersistenceConfigRequest;
import ai.traceable.runner.logs.config.service.v1.UpdateRunnerLogsPurgeConfigRequest;
import com.google.protobuf.Duration;
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
class RunnerLogsConfigServiceRequestValidatorTest {

  private static final String TEST_TENANT_ID = "tenant-id";
  public static final String TENANT_ID_TEXT = "Tenant ID";

  @Mock private RequestContext mockRequestContext;
  private final RunnerLogsConfigServiceRequestValidator requestValidator =
      new RunnerLogsConfigServiceRequestValidator();

  @Test
  void testValidateGetRunnerLogsPurgeConfigRequest() {
    assertInvalidArgStatus(
        TENANT_ID_TEXT,
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, GetRunnerLogsPurgeConfigRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, GetRunnerLogsPurgeConfigRequest.getDefaultInstance()));
  }

  @Test
  void testValidateGetRunnerLogsPersistenceConfigRequest() {
    assertInvalidArgStatus(
        TENANT_ID_TEXT,
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, GetRunnerLogsPersistenceConfigRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, GetRunnerLogsPersistenceConfigRequest.getDefaultInstance()));
  }

  @Test
  void testUpdateRunnerLogsPurgeConfig() {
    assertInvalidArgStatus(
        TENANT_ID_TEXT,
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, UpdateRunnerLogsPurgeConfigRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatus(
        "Expected retention duration to be set in ast scan logs purge config",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                UpdateRunnerLogsPurgeConfigRequest.newBuilder()
                    .setRunnerLogsPurgeConfigUpdate(
                        RunnerLogsPurgeConfigUpdate.newBuilder()
                            .setAstScanLogsPurgeConfig(AstScanLogsPurgeConfigUpdate.newBuilder()))
                    .build()));

    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                UpdateRunnerLogsPurgeConfigRequest.newBuilder()
                    .setRunnerLogsPurgeConfigUpdate(
                        RunnerLogsPurgeConfigUpdate.newBuilder()
                            .setAstScanLogsPurgeConfig(
                                AstScanLogsPurgeConfigUpdate.newBuilder()
                                    .setRetentionDuration(
                                        Duration.newBuilder().setSeconds(10000L))))
                    .build()));
  }

  @Test
  void testUpdateRunnerLogsPersistenceConfig() {
    assertInvalidArgStatus(
        TENANT_ID_TEXT,
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, UpdateRunnerLogsPersistenceConfigRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertInvalidArgStatus(
        "Expected non zero max persist log lines to be set in ast scan logs persistence config",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                UpdateRunnerLogsPersistenceConfigRequest.newBuilder()
                    .setRunnerLogsPersistenceConfigUpdate(
                        RunnerLogsPersistenceConfigUpdate.newBuilder()
                            .setAstScanLogsPersistenceConfig(
                                AstScanLogsPersistenceConfigUpdate.newBuilder()
                                    .setMaxPersistLogLines(0)))
                    .build()));

    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                UpdateRunnerLogsPersistenceConfigRequest.newBuilder()
                    .setRunnerLogsPersistenceConfigUpdate(
                        RunnerLogsPersistenceConfigUpdate.newBuilder()
                            .setAstScanLogsPersistenceConfig(
                                AstScanLogsPersistenceConfigUpdate.getDefaultInstance()))
                    .build()));

    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                UpdateRunnerLogsPersistenceConfigRequest.newBuilder()
                    .setRunnerLogsPersistenceConfigUpdate(
                        RunnerLogsPersistenceConfigUpdate.newBuilder()
                            .setAstScanLogsPersistenceConfig(
                                AstScanLogsPersistenceConfigUpdate.newBuilder()
                                    .setMaxPersistLogLines(100)))
                    .build()));
  }

  private void assertInvalidArgStatus(final String text, final Executable executable) {
    StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, executable);
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    assertTrue(
        Objects.requireNonNull(exception.getStatus().getDescription()).contains(text),
        "Expected arg to contain " + text + " but was " + exception.getMessage());
  }
}
