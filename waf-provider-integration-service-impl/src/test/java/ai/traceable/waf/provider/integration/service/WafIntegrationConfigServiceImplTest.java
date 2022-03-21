package ai.traceable.waf.provider.integration.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import ai.traceable.waf.integration.service.api.v1.CloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.DeleteWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsResponse;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WafProviderServiceGrpc;
import ai.traceable.waf.integration.service.api.v1.WafProviderServiceGrpc.WafProviderServiceBlockingStub;
import com.typesafe.config.Config;
import io.grpc.Status;
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WafIntegrationConfigServiceImplTest {
  MockGenericConfigService mockGenericConfigService;
  Config mockConfig;
  WafProviderServiceBlockingStub wafProviderServiceBlockingStub;

  @BeforeEach
  void setup() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockConfig = mock(Config.class);
    mockGenericConfigService
        .addService(
            new WafIntegrationConfigServiceImpl(
                new WafIntegrationStore(genericStub, configChangeEventGenerator),
                new WafIntegrationConfigRequestValidator()))
        .start();
    this.wafProviderServiceBlockingStub =
        WafProviderServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
  }

  @AfterEach
  void tearDown() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void createWafIntegrationTest() {
    WafIntegrationDetails expectedDetails = createWafIntegrationDetails("name", "email");
    CreateWafIntegrationRequest request =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse response =
        wafProviderServiceBlockingStub.createWafIntegration(request);
    assertEquals(expectedDetails, response.getWafIntegration().getWafIntegrationDetails());
  }

  @Test
  void getWafIntegrationTest() {
    WafIntegrationDetails expectedDetails = createWafIntegrationDetails("name", "email");
    CreateWafIntegrationRequest request =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse response =
        wafProviderServiceBlockingStub.createWafIntegration(request);
    String id = response.getWafIntegration().getId();
    GetWafIntegrationResponse getResponse =
        wafProviderServiceBlockingStub.getWafIntegration(
            GetWafIntegrationRequest.newBuilder().setId(id).build());
    assertEquals(expectedDetails, response.getWafIntegration().getWafIntegrationDetails());
  }

  @Test
  void getWafIntegrationsTest() {
    WafIntegrationDetails expectedDetails1 = createWafIntegrationDetails("name1", "email1");
    CreateWafIntegrationRequest createRequest1 =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails1).build();
    CreateWafIntegrationResponse createResponse1 =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest1);
    String id1 = createResponse1.getWafIntegration().getId();
    WafIntegrationDetails expectedDetails2 = createWafIntegrationDetails("name2", "email2");
    CreateWafIntegrationRequest createRequest2 =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails2).build();
    wafProviderServiceBlockingStub.createWafIntegration(createRequest2);
    WafIntegrationDetails expectedDetails3 = createWafIntegrationDetails("name3", "email3");
    CreateWafIntegrationRequest createRequest3 =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails3).build();
    CreateWafIntegrationResponse createResponse3 =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest3);
    String id3 = createResponse3.getWafIntegration().getId();

    GetWafIntegrationsRequest request =
        GetWafIntegrationsRequest.newBuilder()
            .setFilter(
                GetWafIntegrationsFilter.newBuilder()
                    .addAllIds(List.of(id1, id3))
                    .addWafProviderTypes(
                        GetWafIntegrationsFilter.WafProviderType.WAF_PROVIDER_TYPE_CLOUDFLARE))
            .build();
    GetWafIntegrationsResponse response =
        wafProviderServiceBlockingStub.getWafIntegrations(request);
    assertEquals(2, response.getWafIntegrationCount());
    assertEquals(
        expectedDetails1, response.getWafIntegrationList().get(1).getWafIntegrationDetails());
    assertEquals(
        expectedDetails3, response.getWafIntegrationList().get(0).getWafIntegrationDetails());

    // empty filter case
    GetWafIntegrationsRequest request2 =
        GetWafIntegrationsRequest.newBuilder()
            .setFilter(GetWafIntegrationsFilter.getDefaultInstance())
            .build();
    GetWafIntegrationsResponse response2 =
        wafProviderServiceBlockingStub.getWafIntegrations(request2);
    assertEquals(3, response2.getWafIntegrationCount());
  }

  @Test
  void updateWafIntegrationTest() {
    WafIntegrationDetails details = createWafIntegrationDetails("name", "email");
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(details).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();

    WafIntegrationDetails updatedDetails = createWafIntegrationDetails("name1", "email1");
    UpdateWafIntegrationRequest updateRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId(id)
            .setWafIntegrationDetails(updatedDetails)
            .build();
    UpdateWafIntegrationResponse updateResponse =
        wafProviderServiceBlockingStub.updateWafIntegration(updateRequest);
    assertEquals(updatedDetails, updateResponse.getWafIntegration().getWafIntegrationDetails());
  }

  @Test
  void deleteWafIntegrationTest() {
    WafIntegrationDetails details = createWafIntegrationDetails("name", "email");
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(details).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();

    DeleteWafIntegrationRequest deleteRequest =
        DeleteWafIntegrationRequest.newBuilder().setId(id).build();
    wafProviderServiceBlockingStub.deleteWafIntegration(deleteRequest);

    GetWafIntegrationRequest getRequest = GetWafIntegrationRequest.newBuilder().setId(id).build();
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              wafProviderServiceBlockingStub.getWafIntegration(getRequest);
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  private WafIntegrationDetails createWafIntegrationDetails(String name, String email) {
    return WafIntegrationDetails.newBuilder()
        .setName(name)
        .setDescription("des")
        .setCloudflareIntegrationParams(
            CloudflareIntegrationParams.newBuilder()
                .setApiToken("apitoken")
                .setEmail(email)
                .setZone("zone"))
        .build();
  }
}
