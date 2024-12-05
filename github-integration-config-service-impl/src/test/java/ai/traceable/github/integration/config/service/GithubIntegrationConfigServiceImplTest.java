package ai.traceable.github.integration.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.github.integration.config.service.v1.GetGithubIntegrationsRequest;
import ai.traceable.github.integration.config.service.v1.GithubIntegration;
import ai.traceable.github.integration.config.service.v1.GithubIntegrationConfigServiceGrpc;
import ai.traceable.github.integration.config.service.v1.GithubIntegrationConfigServiceGrpc.GithubIntegrationConfigServiceBlockingStub;
import ai.traceable.github.integration.config.service.v1.GithubIntegrationFilter;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
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
  GithubIntegrationStore store;
  GithubIntegrationConfigServiceBlockingStub stub;

  @BeforeEach
  void setUp() {
    mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGetAll();
    store =
        new GithubIntegrationStore(
            ConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel()),
            mockConfigChangeEventGenerator);
    mockGenericConfigService
        .addService(
            new GithubIntegrationConfigServiceImpl(
                new GithubIntegrationConfigServiceValidator(), store))
        .start();

    stub = GithubIntegrationConfigServiceGrpc.newBlockingStub(mockGenericConfigService.channel());
  }

  @AfterEach
  void tearDown() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void getGithubIntegrations() {
    RequestContext requestContext = RequestContext.forTenantId(TENANT_ID);
    GithubIntegration firstIntegration =
        GithubIntegration.newBuilder()
            .setId("1st")
            .setInstallationId(1)
            .setInstallationOwnerName("FIRST")
            .build();
    GithubIntegration secondIntegration =
        GithubIntegration.newBuilder()
            .setId("2nd")
            .setInstallationId(2)
            .setInstallationOwnerName("second")
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
                        GithubIntegrationFilter.newBuilder().setInstallationOwnerName("first"))
                    .build())
            .getIntegrationsList());
    assertEquals(
        List.of(secondIntegration),
        stub.getGithubIntegrations(
                GetGithubIntegrationsRequest.newBuilder()
                    .setFilter(
                        GithubIntegrationFilter.newBuilder().setInstallationOwnerName("SECOND"))
                    .build())
            .getIntegrationsList());
  }
}
