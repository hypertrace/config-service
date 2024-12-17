package ai.traceable.github.integration.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.github.integration.config.service.v1.CreateGithubIntegrationRequest;
import ai.traceable.github.integration.config.service.v1.DeleteGithubIntegrationRequest;
import ai.traceable.github.integration.config.service.v1.GetGithubIntegrationsRequest;
import ai.traceable.github.integration.config.service.v1.GithubIntegration;
import ai.traceable.github.integration.config.service.v1.GithubIntegrationConfigServiceGrpc;
import ai.traceable.github.integration.config.service.v1.GithubIntegrationConfigServiceGrpc.GithubIntegrationConfigServiceBlockingStub;
import ai.traceable.github.integration.config.service.v1.GithubIntegrationFilter;
import ai.traceable.github.integration.config.service.v1.IntegrationStatus;
import ai.traceable.github.integration.config.service.v1.IntegrationStatus.AwaitingApproval;
import ai.traceable.github.integration.config.service.v1.IntegrationStatus.Completed;
import ai.traceable.github.integration.config.service.v1.UpdateGithubIntegrationRequest;
import java.util.Collections;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GithubIntegrationConfigServiceImplTest {
  public static final String TENANT_ID = "github test tenant";
  MockGenericConfigService mockGenericConfigService;
  @Mock ConfigChangeEventGenerator mockConfigChangeEventGenerator;
  @Mock UuidGenerator mockIdGenerator;
  GithubIntegrationStore store;
  GithubIntegrationConfigServiceBlockingStub stub;

  @BeforeEach
  void setUp() {
    mockGenericConfigService = new MockGenericConfigService();
    store =
        new GithubIntegrationStore(
            ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel()),
            mockConfigChangeEventGenerator);
    mockGenericConfigService
        .addService(
            new GithubIntegrationConfigServiceImpl(
                new GithubIntegrationConfigServiceValidator(), store, mockIdGenerator))
        .start();

    stub =
        GithubIntegrationConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @AfterEach
  void tearDown() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void getGithubIntegrations() {
    mockGenericConfigService.mockUpsert().mockGetAll();
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    GithubIntegration firstIntegration =
        GithubIntegration.newBuilder()
            .setId("1st")
            .setStatus(
                IntegrationStatus.newBuilder()
                    .setCompleted(
                        Completed.newBuilder()
                            .setInstallationId(1)
                            .setGithubInstallationTargetName("FIRST")))
            .build();
    GithubIntegration secondIntegration =
        GithubIntegration.newBuilder()
            .setId("2nd")
            .setStatus(
                IntegrationStatus.newBuilder()
                    .setAwaitingApproval(
                        AwaitingApproval.newBuilder().setGithubInstallationTargetName("second")))
            .build();
    store.upsertObject(requestContext, firstIntegration);
    store.upsertObject(requestContext, secondIntegration);

    assertEquals(
        List.of(secondIntegration, firstIntegration),
        stub.getGithubIntegrations(GetGithubIntegrationsRequest.getDefaultInstance())
            .getIntegrationsList());
    assertEquals(
        List.of(firstIntegration),
        stub.getGithubIntegrations(
                GetGithubIntegrationsRequest.newBuilder()
                    .setFilter(
                        GithubIntegrationFilter.newBuilder()
                            .setGithubInstallationTargetName("first"))
                    .build())
            .getIntegrationsList());
    assertEquals(
        List.of(secondIntegration),
        stub.getGithubIntegrations(
                GetGithubIntegrationsRequest.newBuilder()
                    .setFilter(
                        GithubIntegrationFilter.newBuilder()
                            .setGithubInstallationTargetName("SECOND"))
                    .build())
            .getIntegrationsList());
  }

  @Test
  void createUpdateDeleteIntegration() {
    mockGenericConfigService.mockGet().mockGetAll().mockUpsert().mockDelete();
    RequestContext requestContext = buildTestContext();
    String newId = "new-id";
    when(mockIdGenerator.generateRandomId()).thenReturn(newId);
    GithubIntegration createdIntegration =
        requestContext.call(
            () ->
                stub.createGithubIntegration(CreateGithubIntegrationRequest.getDefaultInstance())
                    .getCreatedIntegration());

    assertEquals(
        "user@email.com",
        createdIntegration.getStatus().getAwaitingRequest().getRequestUserEmail());
    assertEquals(newId, createdIntegration.getId());

    long installId = 123;
    String installOwner = "install-owner";
    GithubIntegration updatedIntegration =
        requestContext
            .call(
                () ->
                    stub.updateGithubIntegration(
                        UpdateGithubIntegrationRequest.newBuilder()
                            .setId(newId)
                            .setStatus(
                                IntegrationStatus.newBuilder()
                                    .setCompleted(
                                        Completed.newBuilder()
                                            .setInstallationId(installId)
                                            .setGithubInstallationUrl(
                                                "https://github.com/apps/traceable/installations/123")
                                            .setGithubInstallationTargetName(installOwner)))
                            .build()))
            .getUpdatedIntegration();

    assertEquals(installId, updatedIntegration.getStatus().getCompleted().getInstallationId());
    assertEquals(
        installOwner,
        updatedIntegration.getStatus().getCompleted().getGithubInstallationTargetName());

    assertEquals(
        List.of(updatedIntegration),
        requestContext.call(
            () ->
                stub.getGithubIntegrations(GetGithubIntegrationsRequest.getDefaultInstance())
                    .getIntegrationsList()));

    requestContext.call(
        () ->
            stub.deleteGithubIntegration(
                DeleteGithubIntegrationRequest.newBuilder().setId(newId).build()));
    assertEquals(
        Collections.emptyList(),
        requestContext.call(
            () ->
                stub.getGithubIntegrations(GetGithubIntegrationsRequest.getDefaultInstance())
                    .getIntegrationsList()));
  }

  private static RequestContext buildTestContext() {
    // {
    //  "sub": "1234567890",
    //  "name": "John Doe",
    //  "email": "user@email.com"
    // }
    return RequestContext.forTenantId("t1")
        .put(
            "authorization",
            "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiZW1haWwiOiJ1c2VyQGVtYWlsLmNvbSJ9.qk7mCQQ6rsuuNqh35NBBLxBAb-bgU1HOAxmk1aAvehc");
  }
}
