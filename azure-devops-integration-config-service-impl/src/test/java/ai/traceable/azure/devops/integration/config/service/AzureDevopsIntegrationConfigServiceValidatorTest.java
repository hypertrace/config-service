package ai.traceable.azure.devops.integration.config.service;

import static org.mockito.Mockito.when;

import ai.traceable.azure.devops.integration.config.service.api.v1.*;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AzureDevopsIntegrationConfigServiceValidatorTest {
  @Mock AzureDevopsIntegrationStore integrationStore;
  @InjectMocks AzureDevopsIntegrationConfigServiceValidator validator;
  private final String TENANT_ID = "tenant-id";
  private final String TEXT = "text";

  void validateCreateAzureDevopsIntegrationRequest() {
    CreateAzureDevopsIntegrationRequest createAzureDevopsIntegrationRequest =
        CreateAzureDevopsIntegrationRequest.newBuilder().build();

    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext1 = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateAzureDevopsIntegration(
                createAzureDevopsIntegrationRequest, requestContext1));

    // Should throw Runtime Exception (no name)
    RequestContext requestContext2 = RequestContext.forTenantId(TENANT_ID);
    CreateAzureDevopsIntegrationRequest createAzureDevopsIntegrationRequest1 =
        CreateAzureDevopsIntegrationRequest.newBuilder(createAzureDevopsIntegrationRequest)
            .setOrganizationUrl(TEXT)
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateAzureDevopsIntegration(
                createAzureDevopsIntegrationRequest1, requestContext2));

    // Should throw Runtime Exception (no org url)
    CreateAzureDevopsIntegrationRequest createAzureDevopsIntegrationRequest2 =
        CreateAzureDevopsIntegrationRequest.newBuilder(createAzureDevopsIntegrationRequest)
            .setName(TEXT)
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateAzureDevopsIntegration(
                createAzureDevopsIntegrationRequest2, requestContext2));

    // Should throw Runtime Exception (no authCredentials)
    CreateAzureDevopsIntegrationRequest createAzureDevopsIntegrationRequest3 =
        minimalCreateAzureDevopsIntegrationRequest(createAzureDevopsIntegrationRequest).toBuilder()
            .clearAzureDevopsIntegrationAuthCredentials()
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateAzureDevopsIntegration(
                createAzureDevopsIntegrationRequest3, requestContext2));

    when(integrationStore.getAllConfigData(requestContext2)).thenReturn(List.of());

    // Should pass with all required fields declared
    CreateAzureDevopsIntegrationRequest createAzureDevopsIntegrationRequest4 =
        minimalCreateAzureDevopsIntegrationRequest(createAzureDevopsIntegrationRequest);
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateCreateAzureDevopsIntegration(
                createAzureDevopsIntegrationRequest4, requestContext2));

    // Should fail for scope without at least one environment
    CreateAzureDevopsIntegrationRequest createAzureDevopsIntegrationRequest5 =
        minimalCreateAzureDevopsIntegrationRequest(createAzureDevopsIntegrationRequest).toBuilder()
            .setAzureDevopsIntegrationScope(
                AzureDevopsIntegrationScope.newBuilder()
                    .setEnvironmentIds(StringList.getDefaultInstance()))
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateAzureDevopsIntegration(
                createAzureDevopsIntegrationRequest5, requestContext2));

    // should pass with all required fields declared and valid environment
    CreateAzureDevopsIntegrationRequest createAzureDevopsIntegrationRequest6 =
        minimalCreateAzureDevopsIntegrationRequest(createAzureDevopsIntegrationRequest).toBuilder()
            .setAzureDevopsIntegrationScope(
                AzureDevopsIntegrationScope.newBuilder()
                    .setEnvironmentIds(StringList.newBuilder().addValues(TEXT))
                    .build())
            .build();
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateCreateAzureDevopsIntegration(
                createAzureDevopsIntegrationRequest6, requestContext2));

    // should throw because an environment can be fetched with the set scope
    when(integrationStore.getAllConfigData(
            requestContext2,
            AzureDevopsIntegrationFilter.newBuilder()
                .setScope(createAzureDevopsIntegrationRequest6.getAzureDevopsIntegrationScope())
                .build()))
        .thenReturn(List.of(AzureDevopsIntegrationWithAuthCredentials.getDefaultInstance()));
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateAzureDevopsIntegration(
                createAzureDevopsIntegrationRequest6, requestContext2));

    // should fail for scope of invalid type
    CreateAzureDevopsIntegrationRequest createAzureDevopsIntegrationRequest7 =
        minimalCreateAzureDevopsIntegrationRequest(createAzureDevopsIntegrationRequest).toBuilder()
            .setAzureDevopsIntegrationScope(AzureDevopsIntegrationScope.newBuilder().build())
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateAzureDevopsIntegration(
                createAzureDevopsIntegrationRequest7, requestContext2));

    // should fail due to non unique naming of integration
    when(integrationStore.getAllConfigData(requestContext2))
        .thenReturn(
            List.of(
                AzureDevopsIntegrationWithAuthCredentials.newBuilder()
                    .setIntegrationDetails(AzureDevopsIntegrationDetails.newBuilder().setName(TEXT))
                    .build()));
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateAzureDevopsIntegration(
                createAzureDevopsIntegrationRequest6, requestContext2));
  }

  @Test
  void validateGetAzureDevopsIntegration() {
    GetAzureDevopsIntegrationsRequest getAzureDevopsIntegrationsRequest1 =
        GetAzureDevopsIntegrationsRequest.getDefaultInstance();
    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext1 = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateGetAzureDevopsIntegrations(
                getAzureDevopsIntegrationsRequest1, requestContext1));

    // Should not throw runtime Exception (default empty filter)
    RequestContext requestContext2 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateGetAzureDevopsIntegrations(
                getAzureDevopsIntegrationsRequest1, requestContext2));

    // Should not throw runtime Exception (valid filter)
    GetAzureDevopsIntegrationsRequest getAzureDevopsIntegrationsRequest2 =
        getAzureDevopsIntegrationsRequest1.toBuilder()
            .setFilter(
                AzureDevopsIntegrationFilter.newBuilder()
                    .setScope(
                        AzureDevopsIntegrationScope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues(TEXT)))
                    .build())
            .build();

    Assertions.assertDoesNotThrow(
        () ->
            validator.validateGetAzureDevopsIntegrations(
                getAzureDevopsIntegrationsRequest2, requestContext2));
  }

  @Test
  void validateAzureDevopsIntegrationWithAuthCredentials() {
    GetAzureDevopsIntegrationWithAuthCredentialsRequest
        getAzureDevopsIntegrationWithAuthCredentialsRequest1 =
            GetAzureDevopsIntegrationWithAuthCredentialsRequest.getDefaultInstance();

    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext1 = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateGetAzureDevopsIntegrationWithAuthCredentials(
                getAzureDevopsIntegrationWithAuthCredentialsRequest1, requestContext1));

    // Should throw runtime Exception (no integration id)
    RequestContext requestContext2 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateGetAzureDevopsIntegrationWithAuthCredentials(
                getAzureDevopsIntegrationWithAuthCredentialsRequest1, requestContext2));

    // Should not throw runtime Exception (valid integration id)
    GetAzureDevopsIntegrationWithAuthCredentialsRequest
        getAzureDevopsIntegrationWithAuthCredentialsRequest2 =
            getAzureDevopsIntegrationWithAuthCredentialsRequest1.toBuilder()
                .setIntegrationId(TEXT)
                .build();
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateGetAzureDevopsIntegrationWithAuthCredentials(
                getAzureDevopsIntegrationWithAuthCredentialsRequest2, requestContext2));
  }

  @Test
  void validateGetAzureDevopsIntegrationsWithAuthCredentialsRequest() {
    GetAzureDevopsIntegrationsWithAuthCredentialsRequest
        getAzureDevopsIntegrationsWithAuthCredentialsRequest1 =
            GetAzureDevopsIntegrationsWithAuthCredentialsRequest.getDefaultInstance();

    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext1 = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateGetAzureDevopsIntegrationsWithAuthCredentials(
                getAzureDevopsIntegrationsWithAuthCredentialsRequest1, requestContext1));

    // Should not throw runtime Exception (default empty filter)
    RequestContext requestContext2 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateGetAzureDevopsIntegrationsWithAuthCredentials(
                getAzureDevopsIntegrationsWithAuthCredentialsRequest1, requestContext2));

    // Should not throw runtime Exception (valid filter)
    GetAzureDevopsIntegrationsWithAuthCredentialsRequest
        getAzureDevopsIntegrationsWithAuthCredentialsRequest2 =
            getAzureDevopsIntegrationsWithAuthCredentialsRequest1.toBuilder()
                .setFilter(
                    AzureDevopsIntegrationFilter.newBuilder()
                        .setScope(
                            AzureDevopsIntegrationScope.newBuilder()
                                .setEnvironmentIds(StringList.newBuilder().addValues(TEXT)))
                        .build())
                .build();
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateGetAzureDevopsIntegrationsWithAuthCredentials(
                getAzureDevopsIntegrationsWithAuthCredentialsRequest2, requestContext2));
  }

  @Test
  void validateUpdateAzureDevopsIntegration() {
    UpdateAzureDevopsIntegrationRequest updateAzureDevopsIntegrationRequest1 =
        UpdateAzureDevopsIntegrationRequest.getDefaultInstance();

    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext1 = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateAzureDevopsIntegration(
                updateAzureDevopsIntegrationRequest1, requestContext1));

    // Should throw runtime Exception (no integration id)
    RequestContext requestContext2 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateAzureDevopsIntegration(
                updateAzureDevopsIntegrationRequest1, requestContext2));

    // Should throw runtime Exception (missing name)
    UpdateAzureDevopsIntegrationRequest updateAzureDevopsIntegrationRequest2 =
        updateAzureDevopsIntegrationRequest1.toBuilder()
            .setIntegrationId(TEXT)
            .setOrganizationUrl(TEXT)
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateAzureDevopsIntegration(
                updateAzureDevopsIntegrationRequest2, requestContext2));

    // Should throw runtime Exception (missing organization_url)
    UpdateAzureDevopsIntegrationRequest updateAzureDevopsIntegrationRequest3 =
        updateAzureDevopsIntegrationRequest1.toBuilder()
            .setIntegrationId(TEXT)
            .setName(TEXT)
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateAzureDevopsIntegration(
                updateAzureDevopsIntegrationRequest3, requestContext2));

    // Should not throw runtime Exception (valid integration id and missing authCredentials)
    UpdateAzureDevopsIntegrationRequest updateAzureDevopsIntegrationRequest5 =
        updateAzureDevopsIntegrationRequest1.toBuilder()
            .setIntegrationId(TEXT)
            .setName(TEXT)
            .setOrganizationUrl(TEXT)
            .build();
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateUpdateAzureDevopsIntegration(
                updateAzureDevopsIntegrationRequest5, requestContext2));

    when(integrationStore.getAllConfigData(
            requestContext2,
            AzureDevopsIntegrationFilter.newBuilder()
                .setScope(
                    AzureDevopsIntegrationScope.newBuilder()
                        .setEnvironmentIds(StringList.newBuilder().addValues(TEXT))
                        .build())
                .build()))
        .thenReturn(
            List.of(
                AzureDevopsIntegrationWithAuthCredentials.newBuilder()
                    .setId(TEXT)
                    .setIntegrationDetails(
                        AzureDevopsIntegrationDetails.newBuilder().setName(TEXT).build())
                    .build()));

    // Should pass with integrationId and any one of name, update-scope present
    UpdateAzureDevopsIntegrationRequest updateServiceNowItsmIntegrationRequest6 =
        updateAzureDevopsIntegrationRequest5.toBuilder()
            .setAzureDevopsIntegrationScope(
                AzureDevopsIntegrationScope.newBuilder()
                    .setEnvironmentIds(StringList.newBuilder().addValues(TEXT))
                    .build())
            .build();

    Assertions.assertDoesNotThrow(
        () ->
            validator.validateUpdateAzureDevopsIntegration(
                updateServiceNowItsmIntegrationRequest6, requestContext2));

    // will fail because the name in update request already exists
    when(integrationStore.getAllConfigData(requestContext2))
        .thenReturn(
            List.of(
                AzureDevopsIntegrationWithAuthCredentials.newBuilder()
                    .setId("otherIntegrationId")
                    .setIntegrationDetails(
                        AzureDevopsIntegrationDetails.newBuilder().setName(TEXT).build())
                    .build()));
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateAzureDevopsIntegration(
                updateServiceNowItsmIntegrationRequest6, requestContext2));
  }

  @Test
  void validateDeleteAzureDevopsIntegration() {
    DeleteAzureDevopsIntegrationRequest deleteAzureDevopsIntegrationRequest =
        DeleteAzureDevopsIntegrationRequest.getDefaultInstance();

    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext1 = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateDeleteAzureDevopsIntegration(
                deleteAzureDevopsIntegrationRequest, requestContext1));

    // Should throw runtime Exception (no integration id)
    RequestContext requestContext2 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateDeleteAzureDevopsIntegration(
                deleteAzureDevopsIntegrationRequest, requestContext2));

    // Should not throw runtime Exception (integration id present)
    DeleteAzureDevopsIntegrationRequest deleteAzureDevopsIntegrationRequest2 =
        DeleteAzureDevopsIntegrationRequest.newBuilder().setIntegrationId(TEXT).build();
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateDeleteAzureDevopsIntegration(
                deleteAzureDevopsIntegrationRequest2, requestContext2));
  }

  private CreateAzureDevopsIntegrationRequest minimalCreateAzureDevopsIntegrationRequest(
      CreateAzureDevopsIntegrationRequest createAzureDevopsIntegrationRequest) {
    AzureDevopsIntegrationAuthCredentials authCredentials =
        AzureDevopsIntegrationAuthCredentials.newBuilder()
            .setEncryptedPat(TEXT)
            .setEncryptionKeyId(TEXT)
            .build();
    return CreateAzureDevopsIntegrationRequest.newBuilder(createAzureDevopsIntegrationRequest)
        .setName(TEXT)
        .setOrganizationUrl(TEXT)
        .setAzureDevopsIntegrationAuthCredentials(authCredentials)
        .build();
  }
}
