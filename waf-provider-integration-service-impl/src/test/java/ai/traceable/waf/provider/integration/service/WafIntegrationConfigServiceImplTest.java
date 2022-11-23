package ai.traceable.waf.provider.integration.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import ai.traceable.waf.integration.service.api.v1.AwsIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AwsIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.AwsResource;
import ai.traceable.waf.integration.service.api.v1.CloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.DeleteWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter.WafProviderType;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsResponse;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.UpdatedCloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.UpdatedWafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WafIntegration;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails.IntegrationParamsCase;
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

    // Cloudflare
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.CLOUDFLARE_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest request =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse response =
        wafProviderServiceBlockingStub.createWafIntegration(request);
    assertEquals(expectedDetails, response.getWafIntegration().getWafIntegrationDetails());

    // AWS
    expectedDetails =
        createWafIntegrationDetails("name", "email", IntegrationParamsCase.AWS_INTEGRATION_PARAMS);
    request =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    response = wafProviderServiceBlockingStub.createWafIntegration(request);
    assertEquals(expectedDetails, response.getWafIntegration().getWafIntegrationDetails());
  }

  @Test
  void getWafIntegrationTest() {

    // Cloudflare
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.CLOUDFLARE_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest request =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse response =
        wafProviderServiceBlockingStub.createWafIntegration(request);
    String id = response.getWafIntegration().getId();
    GetWafIntegrationResponse getResponse =
        wafProviderServiceBlockingStub.getWafIntegration(
            GetWafIntegrationRequest.newBuilder().setId(id).build());
    assertEquals(
        WafIntegration.newBuilder().setWafIntegrationDetails(expectedDetails).setId(id).build(),
        getResponse.getWafIntegration());

    // AWS
    expectedDetails =
        createWafIntegrationDetails("name", "email", IntegrationParamsCase.AWS_INTEGRATION_PARAMS);
    request =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    response = wafProviderServiceBlockingStub.createWafIntegration(request);
    id = response.getWafIntegration().getId();
    getResponse =
        wafProviderServiceBlockingStub.getWafIntegration(
            GetWafIntegrationRequest.newBuilder().setId(id).build());
    assertEquals(
        WafIntegration.newBuilder().setWafIntegrationDetails(expectedDetails).setId(id).build(),
        getResponse.getWafIntegration());
  }

  @Test
  void getWafIntegrationsTest() {
    WafIntegrationDetails expectedDetails1 =
        createWafIntegrationDetails(
            "name1", "email1", IntegrationParamsCase.CLOUDFLARE_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest1 =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails1).build();
    CreateWafIntegrationResponse createResponse1 =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest1);
    String id1 = createResponse1.getWafIntegration().getId();
    WafIntegrationDetails expectedDetails2 =
        createWafIntegrationDetails(
            "name2", "email2", IntegrationParamsCase.CLOUDFLARE_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest2 =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails2).build();
    wafProviderServiceBlockingStub.createWafIntegration(createRequest2);
    WafIntegrationDetails expectedDetails3 =
        createWafIntegrationDetails(
            "name3", "email3", IntegrationParamsCase.CLOUDFLARE_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest3 =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails3).build();
    CreateWafIntegrationResponse createResponse3 =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest3);
    String id3 = createResponse3.getWafIntegration().getId();
    WafIntegrationDetails expectedDetails4 =
        createWafIntegrationDetails(
            "name3", "email3", IntegrationParamsCase.AWS_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest4 =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails4).build();
    wafProviderServiceBlockingStub.createWafIntegration(createRequest4);

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

    // AWS filter
    request =
        GetWafIntegrationsRequest.newBuilder()
            .setFilter(
                GetWafIntegrationsFilter.newBuilder()
                    .addWafProviderTypes(WafProviderType.WAF_PROVIDER_TYPE_AWS))
            .build();
    response = wafProviderServiceBlockingStub.getWafIntegrations(request);
    assertEquals(1, response.getWafIntegrationCount());
    assertEquals(
        expectedDetails4, response.getWafIntegrationList().get(0).getWafIntegrationDetails());

    // empty filter case
    GetWafIntegrationsRequest request2 =
        GetWafIntegrationsRequest.newBuilder()
            .setFilter(GetWafIntegrationsFilter.getDefaultInstance())
            .build();
    GetWafIntegrationsResponse response2 =
        wafProviderServiceBlockingStub.getWafIntegrations(request2);
    assertEquals(4, response2.getWafIntegrationCount());
  }

  @Test
  void updateWafIntegrationTest() {

    // Cloudflare
    WafIntegrationDetails details =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.CLOUDFLARE_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(details).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();

    UpdatedWafIntegrationDetails updatedDetails =
        UpdatedWafIntegrationDetails.newBuilder()
            .setName("name1")
            .setDescription("des")
            .setUpdatedCloudflareIntegrationParams(
                UpdatedCloudflareIntegrationParams.newBuilder().setEmail("email1").setZone("zone"))
            .build();
    UpdateWafIntegrationRequest updateRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId(id)
            .setUpdatedWafIntegrationDetails(updatedDetails)
            .build();
    UpdateWafIntegrationResponse updateResponse =
        wafProviderServiceBlockingStub.updateWafIntegration(updateRequest);
    assertEquals("name1", updateResponse.getWafIntegration().getWafIntegrationDetails().getName());
    assertEquals(
        "email1",
        updateResponse
            .getWafIntegration()
            .getWafIntegrationDetails()
            .getCloudflareIntegrationParams()
            .getEmail());

    // AWS
    details =
        createWafIntegrationDetails("name", "email", IntegrationParamsCase.AWS_INTEGRATION_PARAMS);
    createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(details).build();
    createResponse = wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    id = createResponse.getWafIntegration().getId();

    updatedDetails =
        UpdatedWafIntegrationDetails.newBuilder()
            .setName("name1")
            .setDescription("des")
            .setUpdatedAwsIntegrationParams(
                AwsIntegrationUpdateParams.newBuilder()
                    .setAccessKeyId("id-1")
                    .setEncryptedSecretAccessKey("key-1")
                    .addResources(AwsResource.newBuilder().setArn("arn-1").setRegion("region-1")))
            .build();
    updateRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId(id)
            .setUpdatedWafIntegrationDetails(updatedDetails)
            .build();
    updateResponse = wafProviderServiceBlockingStub.updateWafIntegration(updateRequest);
    assertEquals("name1", updateResponse.getWafIntegration().getWafIntegrationDetails().getName());
    assertEquals(
        "id-1",
        updateResponse
            .getWafIntegration()
            .getWafIntegrationDetails()
            .getAwsIntegrationParams()
            .getAccessKeyId());
    assertEquals(
        "key-1",
        updateResponse
            .getWafIntegration()
            .getWafIntegrationDetails()
            .getAwsIntegrationParams()
            .getEncryptedSecretAccessKey());
    assertEquals(
        List.of(AwsResource.newBuilder().setArn("arn-1").setRegion("region-1").build()),
        updateResponse
            .getWafIntegration()
            .getWafIntegrationDetails()
            .getAwsIntegrationParams()
            .getResourcesList());
  }

  @Test
  void deleteWafIntegrationTest() {

    // Cloudflare
    WafIntegrationDetails details =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.CLOUDFLARE_INTEGRATION_PARAMS);
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
            () -> wafProviderServiceBlockingStub.getWafIntegration(getRequest));
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));

    // AWS
    details =
        createWafIntegrationDetails("name", "email", IntegrationParamsCase.AWS_INTEGRATION_PARAMS);
    createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(details).build();
    createResponse = wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    id = createResponse.getWafIntegration().getId();

    deleteRequest = DeleteWafIntegrationRequest.newBuilder().setId(id).build();
    wafProviderServiceBlockingStub.deleteWafIntegration(deleteRequest);

    GetWafIntegrationRequest getRequest1 = GetWafIntegrationRequest.newBuilder().setId(id).build();
    exception =
        assertThrows(
            RuntimeException.class,
            () -> wafProviderServiceBlockingStub.getWafIntegration(getRequest1));
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  private WafIntegrationDetails createWafIntegrationDetails(
      String name, String email, IntegrationParamsCase paramsCase) {
    switch (paramsCase) {
      case CLOUDFLARE_INTEGRATION_PARAMS:
        return WafIntegrationDetails.newBuilder()
            .setName(name)
            .setDescription("des")
            .setCloudflareIntegrationParams(
                CloudflareIntegrationParams.newBuilder()
                    .setApiToken("apitoken")
                    .setEmail(email)
                    .setZone("zone"))
            .build();
      case AWS_INTEGRATION_PARAMS:
        return WafIntegrationDetails.newBuilder()
            .setName(name)
            .setDescription("des")
            .setAwsIntegrationParams(
                AwsIntegrationParams.newBuilder()
                    .setAccessKeyId("id")
                    .setEncryptedSecretAccessKey("secret")
                    .addResources(
                        AwsResource.newBuilder().setArn("arn").setRegion("region").build()))
            .build();
      default:
        throw new RuntimeException();
    }
  }
}
