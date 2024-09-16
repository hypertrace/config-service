package ai.traceable.servicenow.itsm.integration.config.service;

import static org.mockito.Mockito.when;

import ai.traceable.servicenow.itsm.integration.config.service.api.v1.CreateServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationWithAuthCredentialsRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.GetServiceNowItsmIntegrationsWithAuthCredentialsRequest;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowIntegrationDetails;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmAuthCredentials;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationFilter;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationScope;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationWithAuthCredentials;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.StringList;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.UpdateServiceNowItsmIntegrationRequest;
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
class ServiceNowItsmIntegrationConfigServiceValidatorTest {
  @Mock ServiceNowItsmIntegrationStore integrationStore;
  @InjectMocks ServiceNowItsmIntegrationConfigServiceValidator validator;
  private final String TENANT_ID = "tenant-id";
  private final String TEXT = "text";

  @Test
  void validateCreateServiceNowItsmIntegration() {
    CreateServiceNowItsmIntegrationRequest createServiceNowItsmIntegrationRequest =
        CreateServiceNowItsmIntegrationRequest.newBuilder().build();

    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext1 = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateServiceNowItsmIntegration(
                createServiceNowItsmIntegrationRequest, requestContext1));

    // Should throw Runtime Exception (no fields are declared)
    RequestContext requestContext2 = RequestContext.forTenantId(TENANT_ID);
    CreateServiceNowItsmIntegrationRequest createServiceNowItsmIntegrationRequest1 =
        CreateServiceNowItsmIntegrationRequest.newBuilder(createServiceNowItsmIntegrationRequest)
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateServiceNowItsmIntegration(
                createServiceNowItsmIntegrationRequest1, requestContext2));

    // Should throw Runtime Exception (no authCredentials)
    CreateServiceNowItsmIntegrationRequest createServiceNowItsmIntegrationRequest2 =
        minimalCreateServiceNowItsmIntegrationRequest(createServiceNowItsmIntegrationRequest)
            .toBuilder()
            .clearAuthCredentials()
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateServiceNowItsmIntegration(
                createServiceNowItsmIntegrationRequest2, requestContext2));

    when(integrationStore.getAllConfigData(requestContext2)).thenReturn(List.of());

    // Should pass with all required fields declared
    CreateServiceNowItsmIntegrationRequest createServiceNowItsmIntegrationRequest3 =
        minimalCreateServiceNowItsmIntegrationRequest(createServiceNowItsmIntegrationRequest);
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateCreateServiceNowItsmIntegration(
                createServiceNowItsmIntegrationRequest3, requestContext2));

    // Should fail for scope without at least one environment
    CreateServiceNowItsmIntegrationRequest createServiceNowItsmIntegrationRequest4 =
        minimalCreateServiceNowItsmIntegrationRequest(createServiceNowItsmIntegrationRequest)
            .toBuilder()
            .setScope(
                ServiceNowItsmIntegrationScope.newBuilder()
                    .setEnvironmentIds(StringList.getDefaultInstance()))
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateServiceNowItsmIntegration(
                createServiceNowItsmIntegrationRequest4, requestContext2));

    // should pass with all required fields declared and valid environment
    CreateServiceNowItsmIntegrationRequest createServiceNowItsmIntegrationRequest5 =
        minimalCreateServiceNowItsmIntegrationRequest(createServiceNowItsmIntegrationRequest)
            .toBuilder()
            .setScope(
                ServiceNowItsmIntegrationScope.newBuilder()
                    .setEnvironmentIds(StringList.newBuilder().addValues(TEXT))
                    .build())
            .build();
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateCreateServiceNowItsmIntegration(
                createServiceNowItsmIntegrationRequest5, requestContext2));

    // should throw because an environment can be fetched with the set scope
    when(integrationStore.getAllConfigData(
            requestContext2,
            ServiceNowItsmIntegrationFilter.newBuilder()
                .setScope(createServiceNowItsmIntegrationRequest5.getScope())
                .build()))
        .thenReturn(List.of(ServiceNowItsmIntegrationWithAuthCredentials.getDefaultInstance()));
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateServiceNowItsmIntegration(
                createServiceNowItsmIntegrationRequest5, requestContext2));

    // should fail for scope of invalid type
    CreateServiceNowItsmIntegrationRequest createServiceNowItsmIntegrationRequest6 =
        minimalCreateServiceNowItsmIntegrationRequest(createServiceNowItsmIntegrationRequest)
            .toBuilder()
            .setScope(ServiceNowItsmIntegrationScope.newBuilder().build())
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateServiceNowItsmIntegration(
                createServiceNowItsmIntegrationRequest6, requestContext2));

    // should fail due to non unique naming of integration
    when(integrationStore.getAllConfigData(requestContext2))
        .thenReturn(
            List.of(
                ServiceNowItsmIntegrationWithAuthCredentials.newBuilder()
                    .setIntegrationDetails(ServiceNowIntegrationDetails.newBuilder().setName(TEXT))
                    .build()));
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateServiceNowItsmIntegration(
                createServiceNowItsmIntegrationRequest5, requestContext2));
  }

  private CreateServiceNowItsmIntegrationRequest minimalCreateServiceNowItsmIntegrationRequest(
      CreateServiceNowItsmIntegrationRequest createServiceNowItsmIntegrationRequest) {
    ServiceNowItsmAuthCredentials authCredentials =
        ServiceNowItsmAuthCredentials.newBuilder()
            .setClientId(TEXT)
            .setUserName(TEXT)
            .setEncryptedClientSecret(TEXT)
            .setEncryptedUserPassword(TEXT)
            .setEncryptionKeyId(TEXT)
            .build();
    return CreateServiceNowItsmIntegrationRequest.newBuilder(createServiceNowItsmIntegrationRequest)
        .setName(TEXT)
        .setServerUrl(TEXT)
        .setAuthCredentials(authCredentials)
        .build();
  }

  @Test
  void validateGetServiceNowItsmIntegration() {
    GetServiceNowItsmIntegrationRequest getServiceNowItsmIntegrationRequest1 =
        GetServiceNowItsmIntegrationRequest.getDefaultInstance();
    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext1 = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateGetServiceNowItsmIntegration(
                getServiceNowItsmIntegrationRequest1, requestContext1));

    // Should not throw runtime Exception (default empty filter)
    RequestContext requestContext2 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateGetServiceNowItsmIntegration(
                getServiceNowItsmIntegrationRequest1, requestContext2));

    // Should not throw runtime Exception (valid filter)
    GetServiceNowItsmIntegrationRequest getServiceNowItsmIntegrationRequest2 =
        getServiceNowItsmIntegrationRequest1.toBuilder()
            .setFilter(
                ServiceNowItsmIntegrationFilter.newBuilder()
                    .setScope(
                        ServiceNowItsmIntegrationScope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues(TEXT)))
                    .build())
            .build();

    Assertions.assertDoesNotThrow(
        () ->
            validator.validateGetServiceNowItsmIntegration(
                getServiceNowItsmIntegrationRequest2, requestContext2));
  }

  @Test
  void validateGetServiceNowItsmIntegrationWithAuthCredentials() {
    GetServiceNowItsmIntegrationWithAuthCredentialsRequest
        getServiceNowItsmIntegrationWithAuthCredentialsRequest1 =
            GetServiceNowItsmIntegrationWithAuthCredentialsRequest.getDefaultInstance();

    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext1 = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateGetServiceNowItsmIntegrationWithAuthCredentials(
                getServiceNowItsmIntegrationWithAuthCredentialsRequest1, requestContext1));

    // Should throw runtime Exception (no integration id)
    RequestContext requestContext2 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateGetServiceNowItsmIntegrationWithAuthCredentials(
                getServiceNowItsmIntegrationWithAuthCredentialsRequest1, requestContext2));

    // Should not throw runtime Exception (valid integration id)
    GetServiceNowItsmIntegrationWithAuthCredentialsRequest
        getServiceNowItsmIntegrationWithAuthCredentialsRequest2 =
            getServiceNowItsmIntegrationWithAuthCredentialsRequest1.toBuilder()
                .setIntegrationId(TEXT)
                .build();
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateGetServiceNowItsmIntegrationWithAuthCredentials(
                getServiceNowItsmIntegrationWithAuthCredentialsRequest2, requestContext2));
  }

  @Test
  void validateGetServiceNowItsmIntegrationsWithAuthCredentials() {
    GetServiceNowItsmIntegrationsWithAuthCredentialsRequest
        getServiceNowItsmIntegrationWithAuthCredentialsRequest1 =
            GetServiceNowItsmIntegrationsWithAuthCredentialsRequest.getDefaultInstance();

    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext1 = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateGetServiceNowItsmIntegrationsWithAuthCredentials(
                getServiceNowItsmIntegrationWithAuthCredentialsRequest1, requestContext1));

    // Should not throw runtime Exception (default empty filter)
    RequestContext requestContext2 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateGetServiceNowItsmIntegrationsWithAuthCredentials(
                getServiceNowItsmIntegrationWithAuthCredentialsRequest1, requestContext2));

    // Should not throw runtime Exception (valid filter)
    GetServiceNowItsmIntegrationsWithAuthCredentialsRequest
        getServiceNowItsmIntegrationWithAuthCredentialsRequest2 =
            getServiceNowItsmIntegrationWithAuthCredentialsRequest1.toBuilder()
                .setFilter(
                    ServiceNowItsmIntegrationFilter.newBuilder()
                        .setScope(
                            ServiceNowItsmIntegrationScope.newBuilder()
                                .setEnvironmentIds(StringList.newBuilder().addValues(TEXT)))
                        .build())
                .build();
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateGetServiceNowItsmIntegrationsWithAuthCredentials(
                getServiceNowItsmIntegrationWithAuthCredentialsRequest2, requestContext2));
  }

  @Test
  void validateUpdateServiceNowItsmIntegration() {
    UpdateServiceNowItsmIntegrationRequest updateServiceNowItsmIntegrationRequest1 =
        UpdateServiceNowItsmIntegrationRequest.getDefaultInstance();

    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext1 = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateServiceNowItsmIntegration(
                updateServiceNowItsmIntegrationRequest1, requestContext1));

    // Should throw runtime Exception (no integration id)
    RequestContext requestContext2 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateServiceNowItsmIntegration(
                updateServiceNowItsmIntegrationRequest1, requestContext2));

    // Should throw runtime Exception (missing name)
    UpdateServiceNowItsmIntegrationRequest updateServiceNowItsmIntegrationRequest2 =
        updateServiceNowItsmIntegrationRequest1.toBuilder().setIntegrationId(TEXT).build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateServiceNowItsmIntegration(
                updateServiceNowItsmIntegrationRequest2, requestContext2));

    // Should not throw runtime Exception (missing authCredentials)
    UpdateServiceNowItsmIntegrationRequest updateServiceNowItsmIntegrationRequest3 =
        updateServiceNowItsmIntegrationRequest2.toBuilder().setName(TEXT).build();
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateUpdateServiceNowItsmIntegration(
                updateServiceNowItsmIntegrationRequest3, requestContext2));

    when(integrationStore.getAllConfigData(
            requestContext2,
            ServiceNowItsmIntegrationFilter.newBuilder()
                .setScope(
                    ServiceNowItsmIntegrationScope.newBuilder()
                        .setEnvironmentIds(StringList.newBuilder().addValues(TEXT))
                        .build())
                .build()))
        .thenReturn(
            List.of(
                ServiceNowItsmIntegrationWithAuthCredentials.newBuilder()
                    .setId(TEXT)
                    .setIntegrationDetails(
                        ServiceNowIntegrationDetails.newBuilder().setName(TEXT).build())
                    .build()));

    // Should pass with integrationId and any one of name, update-scope present
    UpdateServiceNowItsmIntegrationRequest updateServiceNowItsmIntegrationRequest4 =
        updateServiceNowItsmIntegrationRequest3.toBuilder()
            .setScope(
                ServiceNowItsmIntegrationScope.newBuilder()
                    .setEnvironmentIds(StringList.newBuilder().addValues(TEXT))
                    .build())
            .build();

    Assertions.assertDoesNotThrow(
        () ->
            validator.validateUpdateServiceNowItsmIntegration(
                updateServiceNowItsmIntegrationRequest4, requestContext2));

    // will fail because the name in update request already exists
    when(integrationStore.getAllConfigData(requestContext2))
        .thenReturn(
            List.of(
                ServiceNowItsmIntegrationWithAuthCredentials.newBuilder()
                    .setId("otherIntegrationId")
                    .setIntegrationDetails(
                        ServiceNowIntegrationDetails.newBuilder().setName(TEXT).build())
                    .build()));
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateServiceNowItsmIntegration(
                updateServiceNowItsmIntegrationRequest4, requestContext2));
  }
}
