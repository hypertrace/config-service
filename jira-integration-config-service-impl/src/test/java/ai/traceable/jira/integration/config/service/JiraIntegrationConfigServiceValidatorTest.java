package ai.traceable.jira.integration.config.service;

import static org.mockito.Mockito.when;

import ai.traceable.jira.integration.config.service.api.v1.CreateJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.DeleteJiraIntegrationRequest;
import ai.traceable.jira.integration.config.service.api.v1.EncryptedData;
import ai.traceable.jira.integration.config.service.api.v1.GetJiraIntegrationsRequest;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegration;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationFilter;
import ai.traceable.jira.integration.config.service.api.v1.Scope;
import ai.traceable.jira.integration.config.service.api.v1.StringList;
import ai.traceable.jira.integration.config.service.api.v1.UpdateJiraIntegrationRequest;
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
class JiraIntegrationConfigServiceValidatorTest {
  @Mock JiraIntegrationStore jiraIntegrationStore;
  @InjectMocks JiraIntegrationConfigServiceValidator validator;
  private final String TENANT_ID = "tenant-id";
  private final String TEXT = "text";

  @Test
  void validateCreateJiraIntegration() {
    CreateJiraIntegrationRequest createJiraIntegrationRequest =
        CreateJiraIntegrationRequest.newBuilder().build();

    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateCreateJiraIntegration(createJiraIntegrationRequest, requestContext));

    // Should throw Runtime Exception (no fields declared)
    RequestContext requestContext2 = RequestContext.forTenantId(TENANT_ID);
    CreateJiraIntegrationRequest createRequest =
        CreateJiraIntegrationRequest.newBuilder(createJiraIntegrationRequest).build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateCreateJiraIntegration(createRequest, requestContext2));

    // Should throw Runtime Exception (no encrypted-access-token)
    CreateJiraIntegrationRequest createRequest1 =
        minimalCreateJiraIntegrationRequest(createJiraIntegrationRequest).toBuilder()
            .clearEncryptedAccessToken()
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateCreateJiraIntegration(createRequest1, requestContext2));

    // should pass with all required fields declared
    CreateJiraIntegrationRequest createRequest2 =
        minimalCreateJiraIntegrationRequest(createJiraIntegrationRequest);
    Assertions.assertDoesNotThrow(
        () -> validator.validateCreateJiraIntegration(createRequest2, requestContext2));

    // should fail for scope without at least one environment
    CreateJiraIntegrationRequest createRequest3 =
        minimalCreateJiraIntegrationRequest(createJiraIntegrationRequest).toBuilder()
            .setScope(Scope.newBuilder().setEnvironmentIds(StringList.getDefaultInstance()))
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateCreateJiraIntegration(createRequest3, requestContext2));

    // should pass with all required fields declared and valid environment
    CreateJiraIntegrationRequest createRequest4 =
        minimalCreateJiraIntegrationRequest(createJiraIntegrationRequest).toBuilder()
            .setScope(Scope.newBuilder().setEnvironmentIds(StringList.newBuilder().addValues(TEXT)))
            .build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateCreateJiraIntegration(createRequest4, requestContext2));

    // should throw because an environment can be fetched with the set scope
    when(jiraIntegrationStore.getAllConfigData(
            requestContext2,
            JiraIntegrationFilter.newBuilder().setFilterScope(createRequest4.getScope()).build()))
        .thenReturn(List.of(JiraIntegration.getDefaultInstance()));
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateCreateJiraIntegration(createRequest4, requestContext2));

    // should fail for scope of invalid type
    CreateJiraIntegrationRequest createRequest5 =
        minimalCreateJiraIntegrationRequest(createJiraIntegrationRequest).toBuilder()
            .setScope(Scope.newBuilder().build())
            .build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateCreateJiraIntegration(createRequest5, requestContext2));
  }

  private CreateJiraIntegrationRequest minimalCreateJiraIntegrationRequest(
      CreateJiraIntegrationRequest createJiraIntegrationRequest) {
    return CreateJiraIntegrationRequest.newBuilder(createJiraIntegrationRequest)
        .setConsumerKey(TEXT)
        .setBaseUrl(TEXT)
        .setName(TEXT)
        .setEncryptedAccessToken(
            EncryptedData.newBuilder().setBase64EncryptedValue(TEXT).setKeyId(TEXT))
        .build();
  }

  @Test
  void validateGetJiraIntegration() {
    GetJiraIntegrationsRequest getJiraIntegrationsRequest =
        GetJiraIntegrationsRequest.getDefaultInstance();
    // Should throw runtime Exception (no TenantId)
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () -> validator.validateGetJiraIntegrations(getJiraIntegrationsRequest, requestContext));

    // Should not throw runtime Exception (default empty filter)
    RequestContext requestContext1 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertDoesNotThrow(
        () -> validator.validateGetJiraIntegrations(getJiraIntegrationsRequest, requestContext1));

    // Should not throw runtime Exception (filter with valid environment)
    GetJiraIntegrationsRequest getJiraIntegrationsRequest1 =
        getJiraIntegrationsRequest.toBuilder()
            .setJiraIntegrationFilter(
                JiraIntegrationFilter.newBuilder()
                    .setFilterScope(
                        Scope.newBuilder()
                            .setEnvironmentIds(StringList.newBuilder().addValues(TEXT))))
            .build();
    Assertions.assertDoesNotThrow(
        () -> validator.validateGetJiraIntegrations(getJiraIntegrationsRequest1, requestContext1));
  }

  @Test
  void validateUpdateJiraIntegration() {
    UpdateJiraIntegrationRequest updateJiraIntegrationRequest =
        UpdateJiraIntegrationRequest.newBuilder().build();

    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateJiraIntegration(updateJiraIntegrationRequest, requestContext));

    // Should throw Runtime Exception (no integrationId)
    RequestContext requestContext1 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateJiraIntegration(updateJiraIntegrationRequest, requestContext1));

    // Should throw Runtime Exception (missing name)
    UpdateJiraIntegrationRequest updateJiraIntegrationRequest1 =
        updateJiraIntegrationRequest.toBuilder().setJiraIntegrationId(TEXT).build();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateUpdateJiraIntegration(
                updateJiraIntegrationRequest1, requestContext1));

    // should pass with integrationId and any one of name, description, update-scope present
    UpdateJiraIntegrationRequest updateJiraIntegrationRequest2 =
        updateJiraIntegrationRequest1.toBuilder()
            .setName(TEXT)
            .setScope(Scope.newBuilder().setEnvironmentIds(StringList.newBuilder().addValues(TEXT)))
            .build();
    Assertions.assertDoesNotThrow(
        () ->
            validator.validateUpdateJiraIntegration(
                updateJiraIntegrationRequest2, requestContext1));
  }

  @Test
  void validateDeleteJiraIntegration() {
    DeleteJiraIntegrationRequest deleteJiraIntegrationRequest =
        DeleteJiraIntegrationRequest.newBuilder().build();

    // Should throw Runtime Exception (no tenantId)
    RequestContext requestContext = new RequestContext();
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateDeleteJiraIntegration(deleteJiraIntegrationRequest, requestContext));

    // Should throw Runtime Exception (no integrationId)
    RequestContext requestContext1 = RequestContext.forTenantId(TENANT_ID);
    Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            validator.validateDeleteJiraIntegration(deleteJiraIntegrationRequest, requestContext1));
  }
}
