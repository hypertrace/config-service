package ai.traceable.integration.config.service.Wiz.validation;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import ai.traceable.integration.config.service.wiz.store.WizIntegrationConfigStore;
import ai.traceable.integration.config.service.wiz.v1.CreateWizIntegrationRequest;
import ai.traceable.integration.config.service.wiz.v1.DeleteWizIntegrationRequest;
import ai.traceable.integration.config.service.wiz.v1.EncryptedText;
import ai.traceable.integration.config.service.wiz.v1.GetWizIntegrationSummariesRequest;
import ai.traceable.integration.config.service.wiz.v1.GetWizIntegrationsRequest;
import ai.traceable.integration.config.service.wiz.v1.UpdateWizIntegrationRequest;
import ai.traceable.integration.config.service.wiz.v1.WizIntegrationFilter;
import ai.traceable.integration.config.service.wiz.validation.WizIntegrationConfigRequestValidator;
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
public class WizIntegrationConfigRequestValidatorTest {

  private static final String TEST_TENANT_ID = "tenant-id";
  private WizIntegrationConfigRequestValidator requestValidator;

  @Mock private RequestContext mockRequestContext;
  @Mock WizIntegrationConfigStore mockWizIntegrationConfigStore;

  @BeforeEach
  void setUp() {
    requestValidator = new WizIntegrationConfigRequestValidator(mockWizIntegrationConfigStore);
  }

  @Test
  void GetWizIntegrationSummariesRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, GetWizIntegrationSummariesRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                GetWizIntegrationSummariesRequest.newBuilder()
                    .setFilter(WizIntegrationFilter.newBuilder().addIds("jkiuyr9879").build())
                    .build()));
  }

  @Test
  void GetWizIntegrationRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, GetWizIntegrationsRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                GetWizIntegrationsRequest.newBuilder()
                    .setFilter(WizIntegrationFilter.newBuilder().addIds("jkiuyr9879").build())
                    .build()));
  }

  @Test
  void CreateWizIntegrationRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, CreateWizIntegrationRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));
    assertDoesNotThrow(
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                CreateWizIntegrationRequest.newBuilder()
                    .setName("wiz integration")
                    .setDescription("random des")
                    .setApiEndpointUrl("http://endpoint")
                    .setTokenUrl("http://tokenurl")
                    .setClientId("dskjhf987")
                    .setClientSecret(
                        EncryptedText.newBuilder().setKeyId("dfkjjkl").setValue("89787539").build())
                    .build()));
  }

  @Test
  void DeleteWizIntegrationRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, DeleteWizIntegrationRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "id",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, DeleteWizIntegrationRequest.newBuilder().build()));

    assertThrows(
        StatusRuntimeException.class,
        () -> {
          requestValidator.validateOrThrow(
              mockRequestContext, DeleteWizIntegrationRequest.newBuilder().setId("fsdf").build());
        });
  }

  @Test
  void UpdateWizIntegrationRequest() {
    assertInvalidArgStatusContaining(
        "Tenant ID",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext, UpdateWizIntegrationRequest.getDefaultInstance()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertInvalidArgStatusContaining(
        "name",
        () ->
            requestValidator.validateOrThrow(
                mockRequestContext,
                UpdateWizIntegrationRequest.newBuilder().setId("sdfsdf").build()));

    when(mockRequestContext.getTenantId()).thenReturn(Optional.of(TEST_TENANT_ID));

    assertThrows(
        StatusRuntimeException.class,
        () -> {
          requestValidator.validateOrThrow(
              mockRequestContext,
              UpdateWizIntegrationRequest.newBuilder()
                  .setId("fsdkjf")
                  .setClientId("fsdfs")
                  .setApiEndpointUrl("http://sdfsdf")
                  .setTokenUrl("http://token")
                  .build());
        });
  }

  private void assertInvalidArgStatusContaining(String text, Executable executable) {
    StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, executable);
    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());

    assertTrue(
        Objects.requireNonNull(exception.getStatus().getDescription()).contains(text),
        "Expected arg to contain " + text + " but was " + exception.getMessage());
  }
}
