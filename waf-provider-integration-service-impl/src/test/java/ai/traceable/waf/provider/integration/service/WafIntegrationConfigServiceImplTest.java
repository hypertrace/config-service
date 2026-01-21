package ai.traceable.waf.provider.integration.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import ai.traceable.waf.integration.service.api.v1.AkamaiAuthCredentials;
import ai.traceable.waf.integration.service.api.v1.AkamaiIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.AkamaiIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AkamaiIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.AkamaiPolicyDetails;
import ai.traceable.waf.integration.service.api.v1.AuthCredentials;
import ai.traceable.waf.integration.service.api.v1.AwsIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AwsIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.AwsResource;
import ai.traceable.waf.integration.service.api.v1.AzureAuthCredentials;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.AzureWafPolicyDetails;
import ai.traceable.waf.integration.service.api.v1.AzureWafPolicyType;
import ai.traceable.waf.integration.service.api.v1.BarracudaAuthCredentials;
import ai.traceable.waf.integration.service.api.v1.BarracudaIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.BarracudaIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.BarracudaIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.BarracudaPolicyDetails;
import ai.traceable.waf.integration.service.api.v1.CloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.CustomListDetail;
import ai.traceable.waf.integration.service.api.v1.DeleteWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.EncryptedData;
import ai.traceable.waf.integration.service.api.v1.EncryptedText;
import ai.traceable.waf.integration.service.api.v1.EnvironmentScope;
import ai.traceable.waf.integration.service.api.v1.F5AuthCredentials;
import ai.traceable.waf.integration.service.api.v1.F5IntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.F5IntegrationParams;
import ai.traceable.waf.integration.service.api.v1.F5IntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.F5PolicyDetails;
import ai.traceable.waf.integration.service.api.v1.FortinetApplication;
import ai.traceable.waf.integration.service.api.v1.FortinetAuthCredentials;
import ai.traceable.waf.integration.service.api.v1.FortinetIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.FortinetIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.FortinetIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.FortinetRuleDetails;
import ai.traceable.waf.integration.service.api.v1.GcpAuthCredentials;
import ai.traceable.waf.integration.service.api.v1.GcpIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.GcpIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsDetailsRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsDetailsResponse;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter.WafProviderType;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsResponse;
import ai.traceable.waf.integration.service.api.v1.GlobalSecurityPolicyScope;
import ai.traceable.waf.integration.service.api.v1.ImpervaIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.ImpervaIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.IntegrationActionType;
import ai.traceable.waf.integration.service.api.v1.RuleType;
import ai.traceable.waf.integration.service.api.v1.StringList;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationResponse;
import ai.traceable.waf.integration.service.api.v1.UpdatedCloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.UpdatedWafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WafIntegration;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails.IntegrationParamsCase;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationScope;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationTarget;
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
  void createWafIntegrationGcpTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails("name", "email", IntegrationParamsCase.GCP_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest request =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse response =
        wafProviderServiceBlockingStub.createWafIntegration(request);
    assertEquals(
        stripGcpSecrets(
            expectedDetails.toBuilder()
                .addIntegrationTargets(
                    WafIntegrationTarget.newBuilder()
                        .setRuleTarget(RuleType.RULE_TYPE_IP_RANGE)
                        .build())
                .addIntegrationTargets(
                    WafIntegrationTarget.newBuilder()
                        .setRuleTarget(RuleType.RULE_TYPE_CUSTOM_SIGNATURE)
                        .build())
                .addIntegrationTargets(
                    WafIntegrationTarget.newBuilder()
                        .setRuleTarget(RuleType.RULE_TYPE_THREAT_ACTORS)
                        .build())
                .build()),
        response.getWafIntegration().getWafIntegrationDetails());
  }

  @Test
  void createWafIntegrationF5Test() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails("name", "email", IntegrationParamsCase.F5_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest request =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse response =
        wafProviderServiceBlockingStub.createWafIntegration(request);
    assertEquals(
        stripF5Secrets(
            expectedDetails.toBuilder()
                .addIntegrationTargets(
                    WafIntegrationTarget.newBuilder()
                        .setRuleTarget(RuleType.RULE_TYPE_IP_RANGE)
                        .build())
                .addIntegrationTargets(
                    WafIntegrationTarget.newBuilder()
                        .setRuleTarget(RuleType.RULE_TYPE_THREAT_ACTORS)
                        .build())
                .build()),
        response.getWafIntegration().getWafIntegrationDetails());
  }

  @Test
  void createWafIntegrationAkamaiTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.AKAMAI_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest request =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse response =
        wafProviderServiceBlockingStub.createWafIntegration(request);
    assertEquals(
        stripAkamaiSecrets(
            expectedDetails.toBuilder()
                .addIntegrationTargets(
                    WafIntegrationTarget.newBuilder()
                        .setRuleTarget(RuleType.RULE_TYPE_IP_RANGE)
                        .build())
                .addIntegrationTargets(
                    WafIntegrationTarget.newBuilder()
                        .setRuleTarget(RuleType.RULE_TYPE_CUSTOM_SIGNATURE)
                        .build())
                .addIntegrationTargets(
                    WafIntegrationTarget.newBuilder()
                        .setRuleTarget(RuleType.RULE_TYPE_THREAT_ACTORS)
                        .build())
                .build()),
        response.getWafIntegration().getWafIntegrationDetails());
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
        .addIntegrationTargets(
            WafIntegrationTarget.newBuilder()
                .setRuleTarget(RuleType.RULE_TYPE_CUSTOM_SIGNATURE)
                .build())
        .addIntegrationTargets(
            WafIntegrationTarget.newBuilder().setRuleTarget(RuleType.RULE_TYPE_IP_RANGE).build())
        .addIntegrationTargets(
            WafIntegrationTarget.newBuilder().setRuleTarget(RuleType.RULE_TYPE_REGION).build())
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
  void createWafIntegrationFortinetTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.FORTINET_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest request =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse response =
        wafProviderServiceBlockingStub.createWafIntegration(request);
    assertEquals(
        stripFortinetSecrets(
            expectedDetails.toBuilder()
                .addIntegrationTargets(
                    WafIntegrationTarget.newBuilder()
                        .setRuleTarget(RuleType.RULE_TYPE_IP_RANGE)
                        .build())
                .addIntegrationTargets(
                    WafIntegrationTarget.newBuilder()
                        .setRuleTarget(RuleType.RULE_TYPE_CUSTOM_SIGNATURE)
                        .build())
                .addIntegrationTargets(
                    WafIntegrationTarget.newBuilder()
                        .setRuleTarget(RuleType.RULE_TYPE_THREAT_ACTORS)
                        .build())
                .build()),
        response.getWafIntegration().getWafIntegrationDetails());
  }

  @Test
  void createWafIntegrationBarracudaTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.BARRACUDA_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest request =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse response =
        wafProviderServiceBlockingStub.createWafIntegration(request);
    assertEquals(
        stripBarracudaSecrets(
            expectedDetails.toBuilder()
                .addIntegrationTargets(
                    WafIntegrationTarget.newBuilder()
                        .setRuleTarget(RuleType.RULE_TYPE_IP_RANGE)
                        .build())
                .addIntegrationTargets(
                    WafIntegrationTarget.newBuilder()
                        .setRuleTarget(RuleType.RULE_TYPE_THREAT_ACTORS)
                        .build())
                .build()),
        response.getWafIntegration().getWafIntegrationDetails());
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
                    .addIntegrationTargets(
                        WafIntegrationTarget.newBuilder()
                            .setRuleTarget(RuleType.RULE_TYPE_CUSTOM_SIGNATURE)
                            .build())
                    .addIntegrationTargets(
                        WafIntegrationTarget.newBuilder()
                            .setRuleTarget(RuleType.RULE_TYPE_IP_RANGE)
                            .build())
                    .addIntegrationTargets(
                        WafIntegrationTarget.newBuilder()
                            .setRuleTarget(RuleType.RULE_TYPE_REGION)
                            .build())
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
  void getWafIntegrationsFilterTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name1", "email1", IntegrationParamsCase.CLOUDFLARE_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(expectedDetails).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();

    // do not match environment ids
    GetWafIntegrationsRequest request =
        GetWafIntegrationsRequest.newBuilder()
            .setFilter(
                GetWafIntegrationsFilter.newBuilder()
                    .setWafIntegrationScope(
                        WafIntegrationScope.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("id"))))
                    .addAllIds(List.of(id))
                    .addWafProviderTypes(WafProviderType.WAF_PROVIDER_TYPE_CLOUDFLARE))
            .build();
    GetWafIntegrationsResponse response =
        wafProviderServiceBlockingStub.getWafIntegrations(request);
    assertEquals(0, response.getWafIntegrationCount());

    // environment id list is empty
    request =
        GetWafIntegrationsRequest.newBuilder()
            .setFilter(
                GetWafIntegrationsFilter.newBuilder()
                    .setWafIntegrationScope(
                        WafIntegrationScope.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of())))
                    .addAllIds(List.of(id))
                    .addWafProviderTypes(WafProviderType.WAF_PROVIDER_TYPE_CLOUDFLARE))
            .build();
    response = wafProviderServiceBlockingStub.getWafIntegrations(request);
    assertEquals(0, response.getWafIntegrationCount());

    // matches filter environment ids
    request =
        GetWafIntegrationsRequest.newBuilder()
            .setFilter(
                GetWafIntegrationsFilter.newBuilder()
                    .setWafIntegrationScope(
                        WafIntegrationScope.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of("id1"))))
                    .addAllIds(List.of(id))
                    .addWafProviderTypes(WafProviderType.WAF_PROVIDER_TYPE_CLOUDFLARE))
            .build();
    response = wafProviderServiceBlockingStub.getWafIntegrations(request);
    assertEquals(1, response.getWafIntegrationCount());
    assertEquals(
        expectedDetails, response.getWafIntegrationList().get(0).getWafIntegrationDetails());

    // environment scope is not set in filter (all environment case)
    request =
        GetWafIntegrationsRequest.newBuilder()
            .setFilter(
                GetWafIntegrationsFilter.newBuilder()
                    .setWafIntegrationScope(WafIntegrationScope.newBuilder())
                    .addAllIds(List.of(id))
                    .addWafProviderTypes(WafProviderType.WAF_PROVIDER_TYPE_CLOUDFLARE))
            .build();
    response = wafProviderServiceBlockingStub.getWafIntegrations(request);
    assertEquals(1, response.getWafIntegrationCount());
    assertEquals(
        expectedDetails, response.getWafIntegrationList().get(0).getWafIntegrationDetails());
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
            .addIntegrationTargets(
                WafIntegrationTarget.newBuilder()
                    .setRuleTarget(RuleType.RULE_TYPE_CUSTOM_SIGNATURE)
                    .build())
            .addIntegrationTargets(
                WafIntegrationTarget.newBuilder()
                    .setRuleTarget(RuleType.RULE_TYPE_IP_RANGE)
                    .build())
            .addIntegrationTargets(
                WafIntegrationTarget.newBuilder().setRuleTarget(RuleType.RULE_TYPE_REGION).build())
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
            .addIntegrationTargets(
                WafIntegrationTarget.newBuilder()
                    .setRuleTarget(RuleType.RULE_TYPE_CUSTOM_SIGNATURE)
                    .build())
            .addIntegrationTargets(
                WafIntegrationTarget.newBuilder()
                    .setRuleTarget(RuleType.RULE_TYPE_IP_RANGE)
                    .build())
            .addIntegrationTargets(
                WafIntegrationTarget.newBuilder().setRuleTarget(RuleType.RULE_TYPE_REGION).build())
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
  void getWafIntegrationsGcpTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name1", "email1", IntegrationParamsCase.GCP_INTEGRATION_PARAMS);
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
                    .addWafProviderTypes(WafProviderType.WAF_PROVIDER_TYPE_GCP))
            .build();
    GetWafIntegrationsResponse response =
        wafProviderServiceBlockingStub.getWafIntegrations(request);
    WafIntegration expectedWafIntegration = createResponse.getWafIntegration();
    assertEquals(1, response.getWafIntegrationCount());
    assertEquals(expectedWafIntegration, response.getWafIntegrationList().get(0));
  }

  @Test
  void getWafIntegrationsF5Test() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails("name1", "email1", IntegrationParamsCase.F5_INTEGRATION_PARAMS);
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
                    .addWafProviderTypes(WafProviderType.WAF_PROVIDER_TYPE_F5))
            .build();
    GetWafIntegrationsResponse response =
        wafProviderServiceBlockingStub.getWafIntegrations(request);
    WafIntegration expectedWafIntegration = createResponse.getWafIntegration();
    assertEquals(1, response.getWafIntegrationCount());
    assertEquals(expectedWafIntegration, response.getWafIntegrationList().get(0));
  }

  @Test
  void getWafIntegrationsAkamaiTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name1", "email1", IntegrationParamsCase.AKAMAI_INTEGRATION_PARAMS);
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
                    .addWafProviderTypes(WafProviderType.WAF_PROVIDER_TYPE_AKAMAI))
            .build();
    GetWafIntegrationsResponse response =
        wafProviderServiceBlockingStub.getWafIntegrations(request);
    WafIntegration expectedWafIntegration = createResponse.getWafIntegration();
    assertEquals(1, response.getWafIntegrationCount());
    assertEquals(expectedWafIntegration, response.getWafIntegrationList().get(0));
  }

  @Test
  void getWafIntegrationsFortinetTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name1", "email1", IntegrationParamsCase.FORTINET_INTEGRATION_PARAMS);
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
                    .addWafProviderTypes(WafProviderType.WAF_PROVIDER_TYPE_FORTINET)
                    .build())
            .build();
    GetWafIntegrationsResponse response =
        wafProviderServiceBlockingStub.getWafIntegrations(request);
    WafIntegration expectedWafIntegration = createResponse.getWafIntegration();
    assertEquals(1, response.getWafIntegrationCount());
    assertEquals(expectedWafIntegration, response.getWafIntegrationList().get(0));
  }

  @Test
  void getWafIntegrationsBarracudaTest() {
    WafIntegrationDetails expectedDetails =
        createWafIntegrationDetails(
            "name1", "email1", IntegrationParamsCase.BARRACUDA_INTEGRATION_PARAMS);
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
                    .addWafProviderTypes(WafProviderType.WAF_PROVIDER_TYPE_BARRACUDA)
                    .build())
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
                UpdatedCloudflareIntegrationParams.newBuilder()
                    .setEmail("email1")
                    .setZone("zone")
                    .setRulesetId("rulesetId"))
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
        "rulesetId",
        updateResponse
            .getWafIntegration()
            .getWafIntegrationDetails()
            .getCloudflareIntegrationParams()
            .getRulesetId());
    assertEquals(
        updateWafIntegrationScope,
        updateResponse.getWafIntegration().getWafIntegrationDetails().getWafIntegrationScope());
    assertEquals(
        List.of(
            WafIntegrationTarget.newBuilder().setRuleTarget(RuleType.RULE_TYPE_IP_RANGE).build(),
            WafIntegrationTarget.newBuilder()
                .setRuleTarget(RuleType.RULE_TYPE_THREAT_ACTORS)
                .build()),
        updateResponse.getWafIntegration().getWafIntegrationDetails().getIntegrationTargetsList());
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
            .addIntegrationTargets(
                WafIntegrationTarget.newBuilder()
                    .setRuleTarget(RuleType.RULE_TYPE_THREAT_ACTORS)
                    .build())
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
    assertEquals(
        List.of(
            WafIntegrationTarget.newBuilder()
                .setRuleTarget(RuleType.RULE_TYPE_THREAT_ACTORS)
                .build()),
        updateResponse.getWafIntegration().getWafIntegrationDetails().getIntegrationTargetsList());
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
                    .setAccountId("account-id-1")
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
    assertEquals("account-id-1", impervaIntegrationParams.getAccountId());

    updatedDetails =
        UpdatedWafIntegrationDetails.newBuilder()
            .setName("name2")
            .setDescription("des")
            .setUpdatedImpervaIntegrationParams(
                ImpervaIntegrationUpdateParams.newBuilder()
                    .setApiId("id-2")
                    .setAccountId("account-id-2")
                    .setWebsiteNames(
                        StringList.newBuilder()
                            .addValues("website1.com")
                            .addValues("website2.com")
                            .build())
                    .build())
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
    assertEquals("account-id-2", impervaIntegrationParams.getAccountId());
    assertEquals(
        List.of("website1.com", "website2.com"),
        impervaIntegrationParams.getWebsiteNames().getValuesList());
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
                            .setAzureWafPolicyDetails(
                                AzureWafPolicyDetails.newBuilder()
                                    .setWafPolicyName("policy-name")
                                    .setWafPolicyResourceGroupName("policy-rg-name")
                                    .setAzureWafPolicyType(
                                        AzureWafPolicyType
                                            .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY))
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
    assertEquals("new-client-id", azureIntegrationDetails.getAuthCredentials().getClientId());
    assertTrue(azureIntegrationDetails.getAuthCredentials().getEncryptedClientSecret().isEmpty());
    assertEquals("new-key-id", azureIntegrationDetails.getAuthCredentials().getAccessKeyId());
  }

  @Test
  void updateWafIntegrationF5Test() {
    WafIntegrationDetails details =
        createWafIntegrationDetails("name", "email", IntegrationParamsCase.F5_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(details).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();

    UpdatedWafIntegrationDetails updatedDetails =
        UpdatedWafIntegrationDetails.newBuilder()
            .setName("name1")
            .setDescription("des1")
            .setUpdatedF5IntegrationParams(
                F5IntegrationUpdateParams.newBuilder()
                    .setF5IntegrationDetails(
                        F5IntegrationDetails.newBuilder()
                            .setUrl("https://localhost:8000")
                            .setF5PolicyDetails(
                                F5PolicyDetails.newBuilder()
                                    .setPolicyId("policy2")
                                    .setPolicyName("policyname2")
                                    .build()))
                    .build())
            .build();
    UpdateWafIntegrationRequest updateRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId(id)
            .setUpdatedWafIntegrationDetails(updatedDetails)
            .build();
    UpdateWafIntegrationResponse updateResponse =
        wafProviderServiceBlockingStub.updateWafIntegration(updateRequest);
    F5IntegrationParams f5IntegrationParams =
        updateResponse.getWafIntegration().getWafIntegrationDetails().getF5IntegrationParams();
    assertEquals("name1", updateResponse.getWafIntegration().getWafIntegrationDetails().getName());
    assertEquals(
        "des1", updateResponse.getWafIntegration().getWafIntegrationDetails().getDescription());
    assertEquals("https://localhost:8000", f5IntegrationParams.getF5IntegrationDetails().getUrl());
    assertEquals(
        "policy2",
        f5IntegrationParams.getF5IntegrationDetails().getF5PolicyDetails().getPolicyId());
    assertEquals(
        "policyname2",
        f5IntegrationParams.getF5IntegrationDetails().getF5PolicyDetails().getPolicyName());

    GetWafIntegrationsDetailsResponse wafIntegrationDetails =
        wafProviderServiceBlockingStub.getWafIntegrationsDetails(
            GetWafIntegrationsDetailsRequest.newBuilder()
                .setFilter(
                    GetWafIntegrationsFilter.newBuilder()
                        .addIds(updateResponse.getWafIntegration().getId()))
                .build());
    assertEquals(
        "password",
        wafIntegrationDetails
            .getWafIntegrationList()
            .get(0)
            .getWafIntegrationDetails()
            .getF5IntegrationParams()
            .getF5IntegrationDetails()
            .getF5AuthCredentials()
            .getEncryptedPassword());
    assertEquals(
        "user-name",
        wafIntegrationDetails
            .getWafIntegrationList()
            .get(0)
            .getWafIntegrationDetails()
            .getF5IntegrationParams()
            .getF5IntegrationDetails()
            .getF5AuthCredentials()
            .getEncryptedUserName());
  }

  @Test
  void updateWafIntegrationAkamaiTest() {
    WafIntegrationDetails details =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.AKAMAI_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(details).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();

    UpdatedWafIntegrationDetails updatedDetails =
        UpdatedWafIntegrationDetails.newBuilder()
            .setName("name1")
            .setDescription("des1")
            .setUpdatedAkamaiIntegrationParams(
                AkamaiIntegrationUpdateParams.newBuilder()
                    .setAkamaiIntegrationDetails(
                        AkamaiIntegrationDetails.newBuilder()
                            .setHost("https://localhost:8000")
                            .setAkamaiPolicyDetails(
                                AkamaiPolicyDetails.newBuilder()
                                    .setPolicyId("policy2")
                                    .setAkamaiPolicyConfigurationId("config2")
                                    .setNetworkListId("network2")
                                    .build()))
                    .build())
            .build();
    UpdateWafIntegrationRequest updateRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId(id)
            .setUpdatedWafIntegrationDetails(updatedDetails)
            .build();
    UpdateWafIntegrationResponse updateResponse =
        wafProviderServiceBlockingStub.updateWafIntegration(updateRequest);
    AkamaiIntegrationParams akamaiIntegrationParams =
        updateResponse.getWafIntegration().getWafIntegrationDetails().getAkamaiIntegrationParams();
    assertEquals("name1", updateResponse.getWafIntegration().getWafIntegrationDetails().getName());
    assertEquals(
        "des1", updateResponse.getWafIntegration().getWafIntegrationDetails().getDescription());
    assertEquals(
        "https://localhost:8000", akamaiIntegrationParams.getAkamaiIntegrationDetails().getHost());
    assertEquals(
        "policy2",
        akamaiIntegrationParams
            .getAkamaiIntegrationDetails()
            .getAkamaiPolicyDetails()
            .getPolicyId());
    assertEquals(
        "config2",
        akamaiIntegrationParams
            .getAkamaiIntegrationDetails()
            .getAkamaiPolicyDetails()
            .getAkamaiPolicyConfigurationId());
    assertEquals(
        "network2",
        akamaiIntegrationParams
            .getAkamaiIntegrationDetails()
            .getAkamaiPolicyDetails()
            .getNetworkListId());

    GetWafIntegrationsDetailsResponse wafIntegrationDetails =
        wafProviderServiceBlockingStub.getWafIntegrationsDetails(
            GetWafIntegrationsDetailsRequest.newBuilder()
                .setFilter(
                    GetWafIntegrationsFilter.newBuilder()
                        .addIds(updateResponse.getWafIntegration().getId()))
                .build());
    assertEquals(
        "client-secret",
        wafIntegrationDetails
            .getWafIntegrationList()
            .get(0)
            .getWafIntegrationDetails()
            .getAkamaiIntegrationParams()
            .getAkamaiIntegrationDetails()
            .getAkamaiAuthCredentials()
            .getEncryptedClientSecret());
    assertEquals(
        "access-token",
        wafIntegrationDetails
            .getWafIntegrationList()
            .get(0)
            .getWafIntegrationDetails()
            .getAkamaiIntegrationParams()
            .getAkamaiIntegrationDetails()
            .getAkamaiAuthCredentials()
            .getEncryptedAccessToken());
    assertEquals(
        "client-secret",
        wafIntegrationDetails
            .getWafIntegrationList()
            .get(0)
            .getWafIntegrationDetails()
            .getAkamaiIntegrationParams()
            .getAkamaiIntegrationDetails()
            .getAkamaiAuthCredentials()
            .getEncryptedClientSecret());
  }

  @Test
  void updateWafIntegrationFortinetTest() {
    WafIntegrationDetails details =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.FORTINET_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(details).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();

    UpdatedWafIntegrationDetails updatedDetails =
        UpdatedWafIntegrationDetails.newBuilder()
            .setName("name1")
            .setDescription("des1")
            .setUpdatedFortinetIntegrationParams(
                FortinetIntegrationUpdateParams.newBuilder()
                    .setFortinetIntegrationDetails(
                        FortinetIntegrationDetails.newBuilder()
                            .setFortinetAuthCredentials(
                                FortinetAuthCredentials.newBuilder()
                                    .setEncryptionKeyId("fortinet-encryption-key-id1")
                                    .setEncryptedApiKey("fortinet-encrypted-api-key1")
                                    .build())
                            .setFortinetRuleDetails(
                                FortinetRuleDetails.newBuilder()
                                    .setFortinetApplication(
                                        FortinetApplication.newBuilder()
                                            .setApplicationId("fortinet-application-id1")
                                            .build())
                                    .build())
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

    WafIntegrationDetails updatedWafIntegrationDetails =
        updateResponse.getWafIntegration().getWafIntegrationDetails();
    FortinetIntegrationDetails updatedFortinetIntegrationDetails =
        updatedWafIntegrationDetails.getFortinetIntegrationParams().getFortinetIntegrationDetails();

    assertEquals("name1", updatedWafIntegrationDetails.getName());
    assertEquals("des1", updatedWafIntegrationDetails.getDescription());
    assertEquals(
        "fortinet-application-id1",
        updatedFortinetIntegrationDetails
            .getFortinetRuleDetails()
            .getFortinetApplication()
            .getApplicationId());

    GetWafIntegrationsDetailsResponse wafIntegrationDetails =
        wafProviderServiceBlockingStub.getWafIntegrationsDetails(
            GetWafIntegrationsDetailsRequest.newBuilder()
                .setFilter(
                    GetWafIntegrationsFilter.newBuilder()
                        .addIds(updateResponse.getWafIntegration().getId())
                        .build())
                .build());
    FortinetAuthCredentials fortinetAuthCredentials =
        wafIntegrationDetails
            .getWafIntegrationList()
            .get(0)
            .getWafIntegrationDetails()
            .getFortinetIntegrationParams()
            .getFortinetIntegrationDetails()
            .getFortinetAuthCredentials();
    assertEquals("fortinet-encrypted-api-key1", fortinetAuthCredentials.getEncryptedApiKey());
    assertEquals("fortinet-encryption-key-id1", fortinetAuthCredentials.getEncryptionKeyId());
  }

  @Test
  void updateWafIntegrationBarracudaTest() {
    WafIntegrationDetails details =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.BARRACUDA_INTEGRATION_PARAMS);
    CreateWafIntegrationRequest createRequest =
        CreateWafIntegrationRequest.newBuilder().setWafIntegrationDetails(details).build();
    CreateWafIntegrationResponse createResponse =
        wafProviderServiceBlockingStub.createWafIntegration(createRequest);
    String id = createResponse.getWafIntegration().getId();

    UpdatedWafIntegrationDetails updatedDetails =
        UpdatedWafIntegrationDetails.newBuilder()
            .setName("name1")
            .setDescription("des1")
            .setUpdatedBarracudaIntegrationParams(
                BarracudaIntegrationUpdateParams.newBuilder()
                    .setBarracudaIntegrationDetails(
                        BarracudaIntegrationDetails.newBuilder()
                            .setUrl("https://localhost:8000")
                            .setBarracudaAuthCredentials(
                                BarracudaAuthCredentials.newBuilder()
                                    .setEncryptedUserName("barracuda-encrypted-user-name")
                                    .setEncryptedPassword("barracuda-encrypted-password")
                                    .setEncryptionKeyId("barracuda-encryption-key-id1"))
                            .setBarracudaPolicyDetails(
                                BarracudaPolicyDetails.newBuilder()
                                    .setWebApplicationName("barracuda-updated-web-application-name")
                                    .setContentRuleGroupName(
                                        "barracuda-updated-content-rule-group-name"))
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

    WafIntegrationDetails updatedWafIntegrationDetails =
        updateResponse.getWafIntegration().getWafIntegrationDetails();
    BarracudaIntegrationDetails updatedBarracudaIntegrationDetails =
        updatedWafIntegrationDetails
            .getBarracudaIntegrationParams()
            .getBarracudaIntegrationDetails();

    assertEquals("name1", updatedWafIntegrationDetails.getName());
    assertEquals("des1", updatedWafIntegrationDetails.getDescription());
    assertEquals(
        "barracuda-updated-web-application-name",
        updatedBarracudaIntegrationDetails.getBarracudaPolicyDetails().getWebApplicationName());

    GetWafIntegrationsDetailsResponse wafIntegrationDetails =
        wafProviderServiceBlockingStub.getWafIntegrationsDetails(
            GetWafIntegrationsDetailsRequest.newBuilder()
                .setFilter(
                    GetWafIntegrationsFilter.newBuilder()
                        .addIds(updateResponse.getWafIntegration().getId())
                        .build())
                .build());
    BarracudaAuthCredentials barracudaAuthCredentials =
        wafIntegrationDetails
            .getWafIntegrationList()
            .get(0)
            .getWafIntegrationDetails()
            .getBarracudaIntegrationParams()
            .getBarracudaIntegrationDetails()
            .getBarracudaAuthCredentials();
    assertEquals("barracuda-encryption-key-id1", barracudaAuthCredentials.getEncryptionKeyId());
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

  @Test
  void deleteWafIntegrationF5Test() {
    WafIntegrationDetails details =
        createWafIntegrationDetails("name", "email", IntegrationParamsCase.F5_INTEGRATION_PARAMS);
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
  void deleteWafIntegrationAkamaiTest() {
    WafIntegrationDetails details =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.AKAMAI_INTEGRATION_PARAMS);
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
  void deleteWafIntegrationFortinetTest() {
    WafIntegrationDetails details =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.FORTINET_INTEGRATION_PARAMS);
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
  void deleteWafIntegrationBarracudaTEst() {
    WafIntegrationDetails details =
        createWafIntegrationDetails(
            "name", "email", IntegrationParamsCase.BARRACUDA_INTEGRATION_PARAMS);
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

  private WafIntegrationDetails stripGcpSecrets(WafIntegrationDetails expectedDetails) {
    GcpIntegrationDetails gcpIntegrationDetails =
        expectedDetails.getGcpIntegrationParams().getGcpIntegrationDetails();
    GcpAuthCredentials.Builder gcpAuthCredentialsBuilder =
        gcpIntegrationDetails.toBuilder().getAuthCredentials().toBuilder()
            .clearEncryptedServiceAccountKey();
    GcpIntegrationDetails updatedGcpIntegrationDetails =
        gcpIntegrationDetails.toBuilder().setAuthCredentials(gcpAuthCredentialsBuilder).build();
    GcpIntegrationParams updatedGcpIntegrationParams =
        expectedDetails.getGcpIntegrationParams().toBuilder()
            .setGcpIntegrationDetails(updatedGcpIntegrationDetails)
            .build();
    return expectedDetails.toBuilder().setGcpIntegrationParams(updatedGcpIntegrationParams).build();
  }

  private WafIntegrationDetails stripF5Secrets(WafIntegrationDetails expectedDetails) {
    F5IntegrationDetails f5IntegrationDetails =
        expectedDetails.getF5IntegrationParams().getF5IntegrationDetails();
    F5IntegrationDetails.Builder f5IntegrationDetailsBuilder =
        f5IntegrationDetails.toBuilder().clearF5AuthCredentials();
    F5IntegrationParams updatedF5IntegrationParams =
        expectedDetails.getF5IntegrationParams().toBuilder()
            .setF5IntegrationDetails(f5IntegrationDetailsBuilder.build())
            .build();
    return expectedDetails.toBuilder().setF5IntegrationParams(updatedF5IntegrationParams).build();
  }

  private WafIntegrationDetails stripAkamaiSecrets(WafIntegrationDetails expectedDetails) {
    AkamaiIntegrationDetails akamaiIntegrationDetails =
        expectedDetails.getAkamaiIntegrationParams().getAkamaiIntegrationDetails();
    AkamaiIntegrationDetails.Builder akamaiIntegrationDetailsBuilder =
        akamaiIntegrationDetails.toBuilder().clearAkamaiAuthCredentials();
    AkamaiIntegrationParams updatedAkamaiIntegrationParams =
        expectedDetails.getAkamaiIntegrationParams().toBuilder()
            .setAkamaiIntegrationDetails(akamaiIntegrationDetailsBuilder.build())
            .build();
    return expectedDetails.toBuilder()
        .setAkamaiIntegrationParams(updatedAkamaiIntegrationParams)
        .build();
  }

  private WafIntegrationDetails stripFortinetSecrets(WafIntegrationDetails expectedDetails) {
    FortinetIntegrationDetails fortinetIntegrationDetails =
        expectedDetails.getFortinetIntegrationParams().getFortinetIntegrationDetails();
    FortinetAuthCredentials.Builder fortinetAuthCredentialsBuilder =
        fortinetIntegrationDetails.toBuilder().getFortinetAuthCredentials().toBuilder()
            .clearEncryptedApiKey();
    FortinetIntegrationDetails updatedFortinetIntegrationDetails =
        fortinetIntegrationDetails.toBuilder()
            .setFortinetAuthCredentials(fortinetAuthCredentialsBuilder)
            .build();
    FortinetIntegrationParams updatedFortinetIntegrationParams =
        expectedDetails.getFortinetIntegrationParams().toBuilder()
            .setFortinetIntegrationDetails(updatedFortinetIntegrationDetails)
            .build();
    return expectedDetails.toBuilder()
        .setFortinetIntegrationParams(updatedFortinetIntegrationParams)
        .build();
  }

  private WafIntegrationDetails stripBarracudaSecrets(WafIntegrationDetails expectedDetails) {
    BarracudaIntegrationDetails barracudaIntegrationDetails =
        expectedDetails.getBarracudaIntegrationParams().getBarracudaIntegrationDetails();
    BarracudaIntegrationDetails.Builder barracudaIntegrationDetailsBuilder =
        barracudaIntegrationDetails.toBuilder().clearBarracudaAuthCredentials();
    BarracudaIntegrationParams updatedBarracudaIntegrationParams =
        expectedDetails.getBarracudaIntegrationParams().toBuilder()
            .setBarracudaIntegrationDetails(barracudaIntegrationDetailsBuilder.build())
            .build();
    return expectedDetails.toBuilder()
        .setBarracudaIntegrationParams(updatedBarracudaIntegrationParams)
        .build();
  }

  private WafIntegrationDetails createWafIntegrationDetails(
      String name, String email, IntegrationParamsCase paramsCase) {
    switch (paramsCase) {
      case CLOUDFLARE_INTEGRATION_PARAMS:
        return WafIntegrationDetails.newBuilder()
            .setName(name)
            .setDescription("des")
            .setWafIntegrationScope(wafConfigScope)
            .addIntegrationTargets(
                WafIntegrationTarget.newBuilder()
                    .setRuleTarget(RuleType.RULE_TYPE_CUSTOM_SIGNATURE)
                    .build())
            .addIntegrationTargets(
                WafIntegrationTarget.newBuilder()
                    .setRuleTarget(RuleType.RULE_TYPE_IP_RANGE)
                    .build())
            .setCloudflareIntegrationParams(
                CloudflareIntegrationParams.newBuilder()
                    .setEncryptedApiToken(
                        EncryptedData.newBuilder()
                            .setKeyId("keyId")
                            .setBase64EncryptedData("apitoken")
                            .build())
                    .setEmail(email)
                    .setRulesetId("rulesetId")
                    .setCustomListDetail(
                        CustomListDetail.newBuilder()
                            .setAllowListName("allowList")
                            .setBlockListName("blockList")
                            .build())
                    .setZone("zone"))
            .build();
      case AWS_INTEGRATION_PARAMS:
        return WafIntegrationDetails.newBuilder()
            .setName(name)
            .setDescription("des")
            .setWafIntegrationScope(wafConfigScope)
            .addIntegrationTargets(
                WafIntegrationTarget.newBuilder()
                    .setRuleTarget(RuleType.RULE_TYPE_CUSTOM_SIGNATURE)
                    .build())
            .addIntegrationTargets(
                WafIntegrationTarget.newBuilder()
                    .setRuleTarget(RuleType.RULE_TYPE_IP_RANGE)
                    .build())
            .addIntegrationTargets(
                WafIntegrationTarget.newBuilder().setRuleTarget(RuleType.RULE_TYPE_REGION).build())
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
            .addIntegrationTargets(
                WafIntegrationTarget.newBuilder()
                    .setRuleTarget(RuleType.RULE_TYPE_IP_RANGE)
                    .build())
            .setImpervaIntegrationParams(
                ImpervaIntegrationParams.newBuilder()
                    .setApiId("id")
                    .setAccountId("account-id")
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
            .addIntegrationTargets(
                WafIntegrationTarget.newBuilder()
                    .setRuleTarget(RuleType.RULE_TYPE_IP_RANGE)
                    .build())
            .setAzureIntegrationParams(
                AzureIntegrationParams.newBuilder()
                    .addAzureIntegrationDetails(
                        AzureIntegrationDetails.newBuilder()
                            .setAzureTenantId("tenant-id")
                            .setSubscriptionId("subscription-id")
                            .setAzureEnvironment("azure-env")
                            .setAzureWafPolicyDetails(
                                AzureWafPolicyDetails.newBuilder()
                                    .setWafPolicyName("policy-name")
                                    .setWafPolicyResourceGroupName("policy-rg-name")
                                    .setAzureWafPolicyType(
                                        AzureWafPolicyType
                                            .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY))
                            .setAuthCredentials(
                                AzureAuthCredentials.newBuilder()
                                    .setClientId("client-id")
                                    .setEncryptedClientSecret("secret")
                                    .setAccessKeyId("key-id")))
                    .build())
            .build();
      case GCP_INTEGRATION_PARAMS:
        return WafIntegrationDetails.newBuilder()
            .setName(name)
            .setDescription("des")
            .setWafIntegrationScope(wafConfigScope)
            .setGcpIntegrationParams(
                GcpIntegrationParams.newBuilder()
                    .setGcpIntegrationDetails(
                        GcpIntegrationDetails.newBuilder()
                            .setProjectId("project-1")
                            .setSecurityPolicyName("policy-1")
                            .setDenyActionResponseCodeValue(400)
                            .setAuthCredentials(
                                GcpAuthCredentials.newBuilder()
                                    .setEncryptedServiceAccountKey(
                                        GcpAuthCredentials.EncryptedText.newBuilder()
                                            .setKeyId("key1")
                                            .setValue("value1")
                                            .build())
                                    .build())
                            .setGlobalSecurityPolicyScope(
                                GlobalSecurityPolicyScope.getDefaultInstance())
                            .build())
                    .build())
            .build();
      case F5_INTEGRATION_PARAMS:
        return WafIntegrationDetails.newBuilder()
            .setName(name)
            .setDescription("des")
            .setWafIntegrationScope(wafConfigScope)
            .setF5IntegrationParams(
                F5IntegrationParams.newBuilder()
                    .setF5IntegrationDetails(
                        F5IntegrationDetails.newBuilder()
                            .setUrl("https://localhost:9000")
                            .setF5PolicyDetails(
                                F5PolicyDetails.newBuilder()
                                    .setPolicyId("policy1")
                                    .setPolicyName("policyname")
                                    .build())
                            .setF5AuthCredentials(
                                F5AuthCredentials.newBuilder()
                                    .setEncryptedUserName("user-name")
                                    .setEncryptedPassword("password")
                                    .setEncryptionKeyId("key-id"))))
            .build();
      case AKAMAI_INTEGRATION_PARAMS:
        return WafIntegrationDetails.newBuilder()
            .setName(name)
            .setDescription("des")
            .setWafIntegrationScope(wafConfigScope)
            .setAkamaiIntegrationParams(
                AkamaiIntegrationParams.newBuilder()
                    .setAkamaiIntegrationDetails(
                        AkamaiIntegrationDetails.newBuilder()
                            .setHost("https://localhost:9000")
                            .setAkamaiPolicyDetails(
                                AkamaiPolicyDetails.newBuilder()
                                    .setPolicyId("policy1")
                                    .setAkamaiPolicyConfigurationId("configId")
                                    .setNetworkListId("network-list-id")
                                    .build())
                            .setAkamaiAuthCredentials(
                                AkamaiAuthCredentials.newBuilder()
                                    .setEncryptedClientSecret("client-secret")
                                    .setEncryptedClientToken("client-token")
                                    .setEncryptedAccessToken("access-token")
                                    .setEncryptionKeyId("key-id"))))
            .build();
      case FORTINET_INTEGRATION_PARAMS:
        return WafIntegrationDetails.newBuilder()
            .setName(name)
            .setDescription("des")
            .setWafIntegrationScope(wafConfigScope)
            .setFortinetIntegrationParams(
                FortinetIntegrationParams.newBuilder()
                    .setFortinetIntegrationDetails(
                        FortinetIntegrationDetails.newBuilder()
                            .setFortinetAuthCredentials(
                                FortinetAuthCredentials.newBuilder()
                                    .setEncryptedApiKey("fortinet-encrypted-api-key")
                                    .setEncryptionKeyId("fortinet-encryption-key-id")
                                    .build())
                            .setFortinetRuleDetails(
                                FortinetRuleDetails.newBuilder()
                                    .setFortinetApplication(
                                        FortinetApplication.newBuilder()
                                            .setApplicationId("fortinet-application-id")
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
      case BARRACUDA_INTEGRATION_PARAMS:
        return WafIntegrationDetails.newBuilder()
            .setName(name)
            .setDescription("des")
            .setWafIntegrationScope(wafConfigScope)
            .setBarracudaIntegrationParams(
                BarracudaIntegrationParams.newBuilder()
                    .setBarracudaIntegrationDetails(
                        BarracudaIntegrationDetails.newBuilder()
                            .setUrl("https://localhost:9000")
                            .setBarracudaPolicyDetails(
                                BarracudaPolicyDetails.newBuilder()
                                    .setWebApplicationName("web-app-name")
                                    .setContentRuleGroupName("content-rule-group-name"))
                            .setBarracudaAuthCredentials(
                                BarracudaAuthCredentials.newBuilder()
                                    .setEncryptionKeyId("barracuda-encrypted-key-id")
                                    .setEncryptedUserName("barracuda-encrypted-user-name")
                                    .setEncryptedPassword("barracuda-encrypted-password"))))
            .build();

      default:
        throw new RuntimeException();
    }
  }
}
