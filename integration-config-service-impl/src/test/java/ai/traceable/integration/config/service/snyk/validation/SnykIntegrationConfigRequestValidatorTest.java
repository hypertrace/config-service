package ai.traceable.integration.config.service.snyk.validation;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.integration.config.service.snyk.v1.CreateSnykIntegrationRequest;
import ai.traceable.integration.config.service.snyk.v1.DeleteSnykIntegrationRequest;
import ai.traceable.integration.config.service.snyk.v1.EncryptedText;
import ai.traceable.integration.config.service.snyk.v1.GetSnykIntegrationDetailsRequest;
import ai.traceable.integration.config.service.snyk.v1.GetSnykIntegrationSummaryRequest;
import ai.traceable.integration.config.service.snyk.v1.UpdateSnykIntegrationRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Objects;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SnykIntegrationConfigRequestValidatorTest {

  private static final String TEST_TENANT_ID = "tenant-id";
  private SnykIntegrationConfigRequestValidator requestValidator;

  @Mock private RequestContext mockRequestContext;

  @BeforeEach
  void setUp() {
    requestValidator = new SnykIntegrationConfigRequestValidator();
  }

  @Test
  void validateGetSnykIntegrationDetailsRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, GetSnykIntegrationDetailsRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, GetSnykIntegrationDetailsRequest.getDefaultInstance()));
  }

  @Test
  void validateGetSnykIntegrationSummaryRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, GetSnykIntegrationSummaryRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, GetSnykIntegrationSummaryRequest.getDefaultInstance()));
  }

  @Test
  void validateCreateSnykIntegrationRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, CreateSnykIntegrationRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "CreateSnykIntegrationRequest.name",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                CreateSnykIntegrationRequest.newBuilder()
                    .setApiToken(
                        EncryptedText.newBuilder().setKeyId("keyId").setValue("value").build())
                    .build()));

    assertInvalidArgStatusContaining(
        "api_token",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                CreateSnykIntegrationRequest.newBuilder().setName("name").build()));

    assertInvalidArgStatusContaining(
        "EncryptedText.key_id",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                CreateSnykIntegrationRequest.newBuilder()
                    .setName("name")
                    .setApiToken(EncryptedText.newBuilder().build())
                    .build()));

    assertInvalidArgStatusContaining(
        "EncryptedText.value",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                CreateSnykIntegrationRequest.newBuilder()
                    .setName("name")
                    .setApiToken(EncryptedText.newBuilder().setKeyId("keyId").build())
                    .build()));

    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                CreateSnykIntegrationRequest.newBuilder()
                    .setName("name")
                    .setApiToken(
                        EncryptedText.newBuilder().setKeyId("keyId").setValue("value").build())
                    .build()));
  }

  @Test
  void validateUpdateSnykIntegrationRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, UpdateSnykIntegrationRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "UpdateSnykIntegrationRequest.name",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, UpdateSnykIntegrationRequest.newBuilder().build()));

    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                UpdateSnykIntegrationRequest.newBuilder().setName("name").build()));

    assertInvalidArgStatusContaining(
        "EncryptedText.key_id",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                UpdateSnykIntegrationRequest.newBuilder()
                    .setName("name")
                    .setApiToken(EncryptedText.newBuilder().build())
                    .build()));

    assertInvalidArgStatusContaining(
        "EncryptedText.value",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                CreateSnykIntegrationRequest.newBuilder()
                    .setName("name")
                    .setApiToken(EncryptedText.newBuilder().setKeyId("keyId").build())
                    .build()));

    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                UpdateSnykIntegrationRequest.newBuilder()
                    .setName("name")
                    .setApiToken(
                        EncryptedText.newBuilder().setKeyId("keyId").setValue("value").build())
                    .build()));
  }

  @Test
  void validateDeleteSnykIntegrationRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, DeleteSnykIntegrationRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, DeleteSnykIntegrationRequest.getDefaultInstance()));
  }

  private void assertInvalidArgStatusContaining(String text, Executable executable) {
    StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, executable);
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    assertTrue(
        Objects.requireNonNull(exception.getStatus().getDescription()).contains(text),
        "Expected arg to contain " + text + " but was " + exception.getMessage());
  }
}
