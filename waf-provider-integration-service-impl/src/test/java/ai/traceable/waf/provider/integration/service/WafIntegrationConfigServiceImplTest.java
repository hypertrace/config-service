package ai.traceable.waf.provider.integration.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import ai.traceable.waf.integration.service.api.v1.AuthCredentials;
import ai.traceable.waf.integration.service.api.v1.AwsIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AwsIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.AwsResource;
import ai.traceable.waf.integration.service.api.v1.AzureApplicationGatewayWafDetails;
import ai.traceable.waf.integration.service.api.v1.AzureAuthCredentials;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.AzureManagedRuleDefinition;
import ai.traceable.waf.integration.service.api.v1.AzureResourceGroupDetails;
import ai.traceable.waf.integration.service.api.v1.CloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.DeleteWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.EncryptedText;
import ai.traceable.waf.integration.service.api.v1.EnvironmentScope;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsDetailsRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsDetailsResponse;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter.WafProviderType;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsResponse;
import ai.traceable.waf.integration.service.api.v1.ImpervaIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.ImpervaIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.IntegrationActionType;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.UpdatedCloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.UpdatedWafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WafIntegration;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails.IntegrationParamsCase;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationScope;
import ai.traceable.waf.integration.service.api.v1.WafProviderServiceGrpc;
import ai.traceable.waf.integration.service.api.v1.WafProviderServiceGrpc.WafProviderServiceBlockingStub;
import ai.traceable.waf.integration.service.api.v1.WebIdentityAuthenticationCredentials;
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
  WafIntegrationScope wafConfigScope =
      WafIntegrationScope.newBuilder()
          .setEnvironmentScope(
              EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("id1", "id2")))
          .build();

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
  void createWafIntegrationCloudflareTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.CLOUDFLARE_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest request =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse response =
        wafProviderServiceBlockingStub.createWafIntegration(request);
    assertEquals(expectedDetails, response.getWafIntegration().getWafIntegrationDetails());
  }

  @Test
  void createWafIntegrationAwsWithAwsAuthTest() {
    WafIntegrationDetails wafIntegrationDetails =
        createWafIntegrationDetails("name", "email", IntegrationParamsCase.AWS_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest request =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(wafIntegrationDetails)
            .build();
    CreateWafIntegrationResponse response =
        wafProviderServiceBlockingStub.createWafIntegration(request);
    WafIntegrationDetails expectedWafIntegrationDetails =
        createAWSWAFIntegrationDetailsWithStripSecrets("name");
    assertEquals(
        expectedWafIntegrationDetails, response.getWafIntegration().getWafIntegrationDetails());
  }

  private WafIntegrationDetails createAWSWAFIntegrationDetailsWithStripSecrets(String name) {
    return WafIntegrationDetails.newBuilder()
        .setName(name)
        .setDescription("des")
        .setWafIntegrationScope(wafConfigScope)
        .setAwsIntegrationParams(
            AwsIntegrationParams.newBuilder()
                .setAuthCredentials(AuthCredentials.newBuilder().setAccessKeyId("id"))
                .setAccessKeyId("id")
                .setRuleGroupCapacity(300)
                .setSyncExistingBlockingData(true)
                .addResources(AwsResource.newBuilder().setArn("arn").setRegion("region").build())
                .setIntegrationActionType(IntegrationActionType.INTEGRATION_ACTION_TYPE_COUNT))
        .build();
  }

  @Test
  void createWafIntegrationAwsWithWebIdentityAuthTest() {
    WafIntegrationDetails expectedDetails =
        createWebIdentityDetails("name", IntegrationParamsCase.AWS_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest request2 =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse response =
        wafProviderServiceBlockingStub.createWafIntegration(request2);
    assertEquals(expectedDetails, response.getWafIntegration().getWafIntegrationDetails());
  }

  @Test
  void createWafIntegrationImpervaTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.IMPERVA_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest request =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse response =
        wafProviderServiceBlockingStub.createWafIntegration(request);
    assertEquals(expectedDetails, response.getWafIntegration().getWafIntegrationDetails());
  }

  @Test
  void getWafIntegrationCloudflareTest() {

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
  }

  @Test
  void getWafIntegrationAWSTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails("name", "email", IntegrationParamsCase.AWS_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest request =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse response =
        wafProviderServiceBlockingStub.createWafIntegration(request);
    String id = response.getWafIntegration().getId();
    GetWafIntegrationResponse getResponse =
        wafProviderServiceBlockingStub.getWafIntegration(
            GetWafIntegrationRequest.newBuilder().setId(id).build());
    assertEquals(
        WafIntegration.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setDescription("des")
                    .setWafIntegrationScope(wafConfigScope)
                    .setAwsIntegrationParams(
                        AwsIntegrationParams.newBuilder()
                            .setAccessKeyId(
                                expectedDetails
                                    .getAwsIntegrationParams()
                                    .getAuthCredentials()
                                    .getAccessKeyId())
                            .setEncryptedSecretAccessKey(
                                expectedDetails
                                    .getAwsIntegrationParams()
                                    .getAuthCredentials()
                                    .getEncryptedSecretAccessKey())
                            .setAuthCredentials(
                                AuthCredentials.newBuilder()
                                    .setAccessKeyId(
                                        expectedDetails
                                            .getAwsIntegrationParams()
                                            .getAuthCredentials()
                                            .getAccessKeyId())
                                    .setEncryptedSecretAccessKey(
                                        expectedDetails
                                            .getAwsIntegrationParams()
                                            .getAuthCredentials()
                                            .getEncryptedSecretAccessKey()))
                            .setRuleGroupCapacity(300)
                            .setSyncExistingBlockingData(true)
                            .setIntegrationActionType(
                                IntegrationActionType.INTEGRATION_ACTION_TYPE_COUNT)
                            .addResources(
                                AwsResource.newBuilder().setArn("arn").setRegion("region"))))
            .setId(id)
            .build(),
        getResponse.getWafIntegration());
  }

  @Test
  void getWafIntegrationImpervaTest() {

    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.IMPERVA_INTEGRATION_PARAMS);
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
  }

  @Test
  void getWafIntegrationsCloudflareTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name1", "email1", IntegrationParamsCase.CLOUDFLARE_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();

    GetWafIntegrationsRequest request =
        GetWafIntegrationsRequest.newBuilder()
            .setFilter(
                GetWafIntegrationsFilter.newBuilder()
                    .addAllIds(List.of(id))
                    .addWafProviderTypes(
                        GetWafIntegrationsFilter.WafProviderType.WAF_PROVIDER_TYPE_CLOUDFLARE))
            .build();
    GetWafIntegrationsResponse response =
        wafProviderServiceBlockingStub.getWafIntegrations(request);
    assertEquals(1, response.getWafIntegrationCount());
    assertEquals(
        expectedDetails, response.getWafIntegrationList().get(0).getWafIntegrationDetails());
  }

  @Test
  void getWafIntegrationsAWSTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name3", "email3", IntegrationParamsCase.AWS_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    wafProviderServiceBlockingStub.createWafIntegration(createRequest);

    GetWafIntegrationsRequest request =
        GetWafIntegrationsRequest.newBuilder()
            .setFilter(
                GetWafIntegrationsFilter.newBuilder()
                    .addWafProviderTypes(WafProviderType.WAF_PROVIDER_TYPE_AWS))
            .build();
    GetWafIntegrationsResponse response =
        wafProviderServiceBlockingStub.getWafIntegrations(request);
    assertEquals(1, response.getWafIntegrationCount());
    assertEquals(
        WafIntegrationDetails.newBuilder()
            .setName("name3")
            .setDescription("des")
            .setWafIntegrationScope(wafConfigScope)
            .setAwsIntegrationParams(
                AwsIntegrationParams.newBuilder()
                    .setAccessKeyId(
                        expectedDetails
                            .getAwsIntegrationParams()
                            .getAuthCredentials()
                            .getAccessKeyId())
                    .setAuthCredentials(
                        AuthCredentials.newBuilder()
                            .setAccessKeyId(
                                expectedDetails
                                    .getAwsIntegrationParams()
                                    .getAuthCredentials()
                                    .getAccessKeyId()))
                    .setRuleGroupCapacity(300)
                    .setSyncExistingBlockingData(true)
                    .setIntegrationActionType(IntegrationActionType.INTEGRATION_ACTION_TYPE_COUNT)
                    .addResources(AwsResource.newBuilder().setArn("arn").setRegion("region")))
            .build(),
        response.getWafIntegrationList().get(0).getWafIntegrationDetails());

    GetWafIntegrationsDetailsRequest request2 =
        GetWafIntegrationsDetailsRequest.newBuilder()
            .setFilter(
                GetWafIntegrationsFilter.newBuilder()
                    .addWafProviderTypes(WafProviderType.WAF_PROVIDER_TYPE_AWS))
            .build();
    GetWafIntegrationsDetailsResponse response2 =
        wafProviderServiceBlockingStub.getWafIntegrationsDetails(request2);
    assertEquals(1, response.getWafIntegrationCount());
    assertEquals(
        WafIntegrationDetails.newBuilder()
            .setName("name3")
            .setDescription("des")
            .setWafIntegrationScope(wafConfigScope)
            .setAwsIntegrationParams(
                AwsIntegrationParams.newBuilder()
                    .setAccessKeyId(
                        expectedDetails
                            .getAwsIntegrationParams()
                            .getAuthCredentials()
                            .getAccessKeyId())
                    .setAuthCredentials(
                        AuthCredentials.newBuilder()
                            .setAccessKeyId(
                                expectedDetails
                                    .getAwsIntegrationParams()
                                    .getAuthCredentials()
                                    .getAccessKeyId())
                            .setEncryptedSecretAccessKey(
                                expectedDetails
                                    .getAwsIntegrationParams()
                                    .getAuthCredentials()
                                    .getEncryptedSecretAccessKey()))
                    .setEncryptedSecretAccessKey(
                        expectedDetails
                            .getAwsIntegrationParams()
                            .getAuthCredentials()
                            .getEncryptedSecretAccessKey())
                    .setRuleGroupCapacity(300)
                    .setSyncExistingBlockingData(true)
                    .setIntegrationActionType(IntegrationActionType.INTEGRATION_ACTION_TYPE_COUNT)
                    .addResources(AwsResource.newBuilder().setArn("arn").setRegion("region")))
            .build(),
        response2.getWafIntegrationList().get(0).getWafIntegrationDetails());
  }

  @Test
  void getWafIntegrationsImpervaTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name1", "email1", IntegrationParamsCase.IMPERVA_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();

    GetWafIntegrationsRequest request =
        GetWafIntegrationsRequest.newBuilder()
            .setFilter(
                GetWafIntegrationsFilter.newBuilder()
                    .addAllIds(List.of(id))
                    .addWafProviderTypes(WafProviderType.WAF_PROVIDER_TYPE_IMPERVA))
            .build();
    GetWafIntegrationsResponse response =
        wafProviderServiceBlockingStub.getWafIntegrations(request);
    assertEquals(1, response.getWafIntegrationCount());
    assertEquals(
        expectedDetails, response.getWafIntegrationList().get(0).getWafIntegrationDetails());
  }

  @Test
  void getWafIntegrationsAzureTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name1", "email1", IntegrationParamsCase.AZURE_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();

    GetWafIntegrationsRequest request =
        GetWafIntegrationsRequest.newBuilder()
            .setFilter(
                GetWafIntegrationsFilter.newBuilder()
                    .addAllIds(List.of(id))
                    .addWafProviderTypes(WafProviderType.WAF_PROVIDER_TYPE_AZURE))
            .build();
    GetWafIntegrationsResponse response =
        wafProviderServiceBlockingStub.getWafIntegrations(request);
    WafIntegration expectedWafIntegration = createResponse.getWafIntegration();
    assertEquals(1, response.getWafIntegrationCount());
    assertEquals(expectedWafIntegration, response.getWafIntegrationList().get(0));
  }

  @Test
  void updateWafIntegrationCloudflareTest() {

    WafIntegrationDetails details =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.CLOUDFLARE_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(details).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();
    WafIntegrationScope updateWafIntegrationScope =
        WafIntegrationScope.newBuilder()
            .setEnvironmentScope(
                EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("updatedId")))
            .build();
    UpdatedWafIntegrationDetails updatedDetails =
        UpdatedWafIntegrationDetails.newBuilder()
            .setName("name1")
            .setDescription("des")
            .setWafIntegrationScope(updateWafIntegrationScope)
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
    assertEquals(
        updateWafIntegrationScope,
        updateResponse.getWafIntegration().getWafIntegrationDetails().getWafIntegrationScope());
  }

  @Test
  void updateWafIntegrationAWSTest() {

    WafIntegrationDetails details =
        createWafIntegrationDetails("name", "email", IntegrationParamsCase.AWS_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(details).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();

    UpdatedWafIntegrationDetails updatedDetails =
        UpdatedWafIntegrationDetails.newBuilder()
            .setName("name1")
            .setDescription("des")
            .setUpdatedAwsIntegrationParams(
                AwsIntegrationUpdateParams.newBuilder()
                    .setAuthCredentials(
                        AuthCredentials.newBuilder()
                            .setAccessKeyId("id-1")
                            .setEncryptedSecretAccessKey("key-1"))
                    .setRuleGroupCapacity(100)
                    .addResources(AwsResource.newBuilder().setArn("arn-1").setRegion("region-1")))
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
        "id-1",
        updateResponse
            .getWafIntegration()
            .getWafIntegrationDetails()
            .getAwsIntegrationParams()
            .getAuthCredentials()
            .getAccessKeyId());
    assertEquals(
        List.of(AwsResource.newBuilder().setArn("arn-1").setRegion("region-1").build()),
        updateResponse
            .getWafIntegration()
            .getWafIntegrationDetails()
            .getAwsIntegrationParams()
            .getResourcesList());
    assertEquals(
        100,
        updateResponse
            .getWafIntegration()
            .getWafIntegrationDetails()
            .getAwsIntegrationParams()
            .getRuleGroupCapacity());
  }

  @Test
  void updateWafIntegrationAwsWebIdentityAuth() {

    WafIntegrationDetails wafIntegrationDetails =
        createWebIdentityDetails("name", IntegrationParamsCase.AWS_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest request =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(wafIntegrationDetails)
            .build();

    CreateWafIntegrationResponse response =
        wafProviderServiceBlockingStub.createWafIntegration(request);
    String id = response.getWafIntegration().getId();

    UpdatedWafIntegrationDetails updatedDetails =
        UpdatedWafIntegrationDetails.newBuilder()
            .setName(wafIntegrationDetails.getName())
            .setDescription(wafIntegrationDetails.getDescription())
            .setUpdatedAwsIntegrationParams(
                AwsIntegrationUpdateParams.newBuilder()
                    .setWebIdentityAuthCredentials(
                        WebIdentityAuthenticationCredentials.newBuilder().setRoleArn("role-arn"))
                    .setRuleGroupCapacity(200)
                    .addAllResources(
                        wafIntegrationDetails.getAwsIntegrationParams().getResourcesList()))
            .build();
    UpdateWafIntegrationRequest updateRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId(id)
            .setUpdatedWafIntegrationDetails(updatedDetails)
            .build();
    UpdateWafIntegrationResponse updateResponse =
        wafProviderServiceBlockingStub.updateWafIntegration(updateRequest);
    assertEquals(
        "role-arn",
        updateResponse
            .getWafIntegration()
            .getWafIntegrationDetails()
            .getAwsIntegrationParams()
            .getWebIdentityAuthCredentials()
            .getRoleArn());
    assertEquals(
        200,
        updateResponse
            .getWafIntegration()
            .getWafIntegrationDetails()
            .getAwsIntegrationParams()
            .getRuleGroupCapacity());
  }

  @Test
  void updateWafIntegrationImpervaTest() {

    WafIntegrationDetails details =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.IMPERVA_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(details).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();

    UpdatedWafIntegrationDetails updatedDetails =
        UpdatedWafIntegrationDetails.newBuilder()
            .setName("name1")
            .setDescription("des1")
            .setUpdatedImpervaIntegrationParams(
                ImpervaIntegrationUpdateParams.newBuilder()
                    .setApiId("id-1")
                    .setApiKey(
                        EncryptedText.newBuilder()
                            .setKeyId("secret-key-id-1")
                            .setValue("secret-value-1")
                            .build())
                    .build())
            .build();
    UpdateWafIntegrationRequest updateRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId(id)
            .setUpdatedWafIntegrationDetails(updatedDetails)
            .build();
    UpdateWafIntegrationResponse updateResponse =
        wafProviderServiceBlockingStub.updateWafIntegration(updateRequest);
    ImpervaIntegrationParams impervaIntegrationParams =
        updateResponse.getWafIntegration().getWafIntegrationDetails().getImpervaIntegrationParams();
    assertEquals("name1", updateResponse.getWafIntegration().getWafIntegrationDetails().getName());
    assertEquals(
        "des1", updateResponse.getWafIntegration().getWafIntegrationDetails().getDescription());
    assertEquals("id-1", impervaIntegrationParams.getApiId());
    assertEquals("secret-key-id-1", impervaIntegrationParams.getApiKey().getKeyId());
    assertEquals("secret-value-1", impervaIntegrationParams.getApiKey().getValue());

    updatedDetails =
        UpdatedWafIntegrationDetails.newBuilder()
            .setName("name2")
            .setDescription("des")
            .setUpdatedImpervaIntegrationParams(
                ImpervaIntegrationUpdateParams.newBuilder().setApiId("id-2"))
            .build();
    updateRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId(id)
            .setUpdatedWafIntegrationDetails(updatedDetails)
            .build();
    updateResponse = wafProviderServiceBlockingStub.updateWafIntegration(updateRequest);
    impervaIntegrationParams =
        updateResponse.getWafIntegration().getWafIntegrationDetails().getImpervaIntegrationParams();
    assertEquals("name2", updateResponse.getWafIntegration().getWafIntegrationDetails().getName());
    assertEquals(
        "des", updateResponse.getWafIntegration().getWafIntegrationDetails().getDescription());
    assertEquals("id-2", impervaIntegrationParams.getApiId());
    assertEquals("secret-key-id-1", impervaIntegrationParams.getApiKey().getKeyId());
    assertEquals("secret-value-1", impervaIntegrationParams.getApiKey().getValue());
  }

  @Test
  void updateWafIntegrationAzureTest() {
    WafIntegrationDetails details =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.AZURE_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(details).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();
    String azureIntegrationDetailsId =
        createResponse
            .getWafIntegration()
            .getWafIntegrationDetails()
            .getAzureIntegrationParams()
            .getAzureIntegrationDetails(0)
            .getId();

    UpdatedWafIntegrationDetails updatedDetails =
        UpdatedWafIntegrationDetails.newBuilder()
            .setName("name1")
            .setDescription("des1")
            .setUpdatedAzureIntegrationParams(
                AzureIntegrationUpdateParams.newBuilder()
                    .addAzureIntegrationDetails(
                        AzureIntegrationDetails.newBuilder()
                            .setId(azureIntegrationDetailsId)
                            .setAzureTenantId("new-tenant-id")
                            .setSubscriptionId("new-subscription-id")
                            .setAzureEnvironment("new-azure-env")
                            .addAzureResourceGroupDetails(
                                AzureResourceGroupDetails.newBuilder()
                                    .setName("new-name")
                                    .setRegion("new-region"))
                            .setApplicationGatewayWafDetails(
                                AzureApplicationGatewayWafDetails.newBuilder()
                                    .setManagedRuleDefinition(
                                        AzureManagedRuleDefinition.newBuilder()
                                            .setRuleSetType("OWASP")
                                            .setRuleSetVersion("3.2")))
                            .setAuthCredentials(
                                AzureAuthCredentials.newBuilder()
                                    .setClientId("new-client-id")
                                    .setAccessKeyId("new-key-id"))))
            .build();

    UpdateWafIntegrationRequest updateRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId(id)
            .setUpdatedWafIntegrationDetails(updatedDetails)
            .build();
    UpdateWafIntegrationResponse updateResponse =
        wafProviderServiceBlockingStub.updateWafIntegration(updateRequest);
    AzureIntegrationDetails azureIntegrationDetails =
        updateResponse
            .getWafIntegration()
            .getWafIntegrationDetails()
            .getAzureIntegrationParams()
            .getAzureIntegrationDetails(0);
    assertEquals("name1", updateResponse.getWafIntegration().getWafIntegrationDetails().getName());
    assertEquals(
        "des1", updateResponse.getWafIntegration().getWafIntegrationDetails().getDescription());
    assertEquals("new-tenant-id", azureIntegrationDetails.getAzureTenantId());
    assertEquals("new-subscription-id", azureIntegrationDetails.getSubscriptionId());
    assertEquals("new-azure-env", azureIntegrationDetails.getAzureEnvironment());
    assertEquals("new-name", azureIntegrationDetails.getAzureResourceGroupDetails(0).getName());
    assertEquals("new-region", azureIntegrationDetails.getAzureResourceGroupDetails(0).getRegion());
    assertEquals("new-client-id", azureIntegrationDetails.getAuthCredentials().getClientId());
    assertTrue(azureIntegrationDetails.getAuthCredentials().getEncryptedClientSecret().isEmpty());
    assertEquals("new-key-id", azureIntegrationDetails.getAuthCredentials().getAccessKeyId());
  }

  @Test
  void deleteWafIntegrationCloudflareTest() {

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
  }

  @Test
  void deleteWafIntegrationAWSTest() {
    WafIntegrationDetails details =
        createWafIntegrationDetails("name", "email", IntegrationParamsCase.AWS_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(details).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();

    DeleteWafIntegrationRequest deleteRequest =
        DeleteWafIntegrationRequest.newBuilder().setId(id).build();
    wafProviderServiceBlockingStub.deleteWafIntegration(deleteRequest);

    GetWafIntegrationRequest getRequest1 = GetWafIntegrationRequest.newBuilder().setId(id).build();
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> wafProviderServiceBlockingStub.getWafIntegration(getRequest1));
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void deleteWafIntegrationImpervaTest() {

    WafIntegrationDetails details =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.IMPERVA_INTEGRATION_PARAMS);
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
  }

  private WafIntegrationDetails createWebIdentityDetails(
      String name, IntegrationParamsCase paramsCase) {
    switch (paramsCase) {
      case AWS_INTEGRATION_PARAMS:
        return WafIntegrationDetails.newBuilder()
            .setName(name)
            .setDescription("des")
            .setAwsIntegrationParams(
                AwsIntegrationParams.newBuilder()
                    .setWebIdentityAuthCredentials(
                        WebIdentityAuthenticationCredentials.newBuilder()
                            .setRoleArn("aws:arn:435485798347"))
                    .addResources(
                        AwsResource.newBuilder().setArn("arn").setRegion("region").build()))
            .build();
      default:
        throw new RuntimeException();
    }
  }

  private WafIntegrationDetails createWafIntegrationDetails(
      String name, String email, IntegrationParamsCase paramsCase) {
    switch (paramsCase) {
      case CLOUDFLARE_INTEGRATION_PARAMS:
        return WafIntegrationDetails.newBuilder()
            .setName(name)
            .setDescription("des")
            .setWafIntegrationScope(wafConfigScope)
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
            .setWafIntegrationScope(wafConfigScope)
            .setAwsIntegrationParams(
                AwsIntegrationParams.newBuilder()
                    .setAuthCredentials(
                        AuthCredentials.newBuilder()
                            .setAccessKeyId("id")
                            .setEncryptedSecretAccessKey("secret"))
                    .setEncryptedSecretAccessKey("secret")
                    .setAccessKeyId("id")
                    .setRuleGroupCapacity(300)
                    .setSyncExistingBlockingData(true)
                    .addResources(
                        AwsResource.newBuilder().setArn("arn").setRegion("region").build())
                    .setIntegrationActionType(IntegrationActionType.INTEGRATION_ACTION_TYPE_COUNT))
            .build();
      case IMPERVA_INTEGRATION_PARAMS:
        return WafIntegrationDetails.newBuilder()
            .setName(name)
            .setDescription("des")
            .setWafIntegrationScope(wafConfigScope)
            .setImpervaIntegrationParams(
                ImpervaIntegrationParams.newBuilder()
                    .setApiId("id")
                    .setApiKey(
                        EncryptedText.newBuilder()
                            .setKeyId("secret-id")
                            .setValue("secret-value")
                            .build())
                    .build())
            .build();
      case AZURE_INTEGRATION_PARAMS:
        return WafIntegrationDetails.newBuilder()
            .setName(name)
            .setDescription("des")
            .setWafIntegrationScope(wafConfigScope)
            .setAzureIntegrationParams(
                AzureIntegrationParams.newBuilder()
                    .addAzureIntegrationDetails(
                        AzureIntegrationDetails.newBuilder()
                            .setAzureTenantId("tenant-id")
                            .setSubscriptionId("subscription-id")
                            .setAzureEnvironment("azure-env")
                            .addAzureResourceGroupDetails(
                                AzureResourceGroupDetails.newBuilder()
                                    .setName("name")
                                    .setRegion("region"))
                            .setApplicationGatewayWafDetails(
                                AzureApplicationGatewayWafDetails.newBuilder()
                                    .setManagedRuleDefinition(
                                        AzureManagedRuleDefinition.newBuilder()
                                            .setRuleSetType("OWASP")
                                            .setRuleSetVersion("3.2")))
                            .setAuthCredentials(
                                AzureAuthCredentials.newBuilder()
                                    .setClientId("client-id")
                                    .setEncryptedClientSecret("secret")
                                    .setAccessKeyId("key-id")))
                    .build())
            .build();
      default:
        throw new RuntimeException();
    }
  }
}
