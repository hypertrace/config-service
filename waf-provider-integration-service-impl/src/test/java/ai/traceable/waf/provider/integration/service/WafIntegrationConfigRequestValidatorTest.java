package ai.traceable.waf.provider.integration.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.waf.integration.service.api.v1.AkamaiAuthCredentials;
import ai.traceable.waf.integration.service.api.v1.AkamaiIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.AkamaiIntegrationParams;
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
import ai.traceable.waf.integration.service.api.v1.CloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.DeleteWafIntegrationRequest;
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
import ai.traceable.waf.integration.service.api.v1.FortinetTemplate;
import ai.traceable.waf.integration.service.api.v1.GcpAuthCredentials;
import ai.traceable.waf.integration.service.api.v1.GcpIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.GcpIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.GcpIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter.WafProviderType;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsRequest;
import ai.traceable.waf.integration.service.api.v1.GlobalSecurityPolicyScope;
import ai.traceable.waf.integration.service.api.v1.ImpervaIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.ImpervaIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.RegionSecurityPolicyScope;
import ai.traceable.waf.integration.service.api.v1.StringList;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.UpdatedCloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.UpdatedWafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WafIntegration;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationScope;
import ai.traceable.waf.integration.service.api.v1.WebIdentityAuthenticationCredentials;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class WafIntegrationConfigRequestValidatorTest {
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("test-tenant");
  public static final String EXISTING_SECURITY_POLICY_NAME = "existingSecurityPolicyName";
  public static final String EXISTING_POLICY_ID = "existing-policy-id";
  public static final String EXSITING_F5_URL = "exsitingF5Url";

  private final WafIntegrationConfigRequestValidator wafIntegrationConfigRequestValidator;
  private List<WafIntegration> existingWafIntegrations = List.of();

  private static final String TENANT_ID = "azureTenantId";
  private static final String SUBSCRIPTION_ID = "azureSubscriptionId";
  private static final String AZURE_ENVIRONMENT = "azureEnvironment";
  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String ACCESS_KEY = "accessKeyId";
  private static final String WAF_POLICY = "wafPolicyName";
  private static final String WAF_POLICY_RESOURCE_GROUP = "wafPolicyResourceGroup";
  private static final String AZURE_WAF_NAME = "azure-waf";
  private static final String WAF_INTEGRATION_ID = "wafIntegrationId";

  private static final String ENCRYPTION_KEY_ID = "fortinet-encryption-key-id";
  private static final String ENCRYPTED_API_KEY = "fortinet-encrypted-api-key";
  private static final String APPLICATION_ID = "fortinet-application-id";
  private static final String TEMPLATE_ID = "fortinet-template-id";

  public WafIntegrationConfigRequestValidatorTest() {
    wafIntegrationConfigRequestValidator = new WafIntegrationConfigRequestValidator();
  }

  @Test
  void invalidWafIntegrationsFilterTest() {
    GetWafIntegrationsRequest request =
        GetWafIntegrationsRequest.newBuilder()
            .setFilter(GetWafIntegrationsFilter.newBuilder().addAllIds(List.of("", "")))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(request, REQUEST_CONTEXT);
        });

    GetWafIntegrationsRequest request2 =
        GetWafIntegrationsRequest.newBuilder()
            .setFilter(
                GetWafIntegrationsFilter.newBuilder()
                    .addAllWafProviderTypes(List.of(WafProviderType.WAF_PROVIDER_TYPE_UNSPECIFIED)))
            .build();

    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(request2, REQUEST_CONTEXT);
        });

    GetWafIntegrationsRequest request3 =
        GetWafIntegrationsRequest.newBuilder()
            .setFilter(
                GetWafIntegrationsFilter.newBuilder()
                    .addAllWafProviderTypes(
                        List.of(
                            WafProviderType.WAF_PROVIDER_TYPE_AWS,
                            WafProviderType.WAF_PROVIDER_TYPE_CLOUDFLARE,
                            WafProviderType.WAF_PROVIDER_TYPE_IMPERVA,
                            WafProviderType.WAF_PROVIDER_TYPE_AZURE)))
            .build();

    assertDoesNotThrow(
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(request3, REQUEST_CONTEXT);
        });
  }

  @Test
  void invalidCloudflareWafIntegrationDetailsTest() {
    CloudflareIntegrationParams cloudflareIntegrationParams =
        CloudflareIntegrationParams.newBuilder()
            .setZone("zone")
            .setEmail("email")
            .setApiToken("apiToken")
            .build();
    // name field is absent
    CreateWafIntegrationRequest request1 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setDescription("des")
                    .setCloudflareIntegrationParams(cloudflareIntegrationParams))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request1, REQUEST_CONTEXT, existingWafIntegrations);
        });
  }

  @Test
  void invalidUpdateCloudflareRequestTest() {
    UpdateWafIntegrationRequest request =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id-1")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setDescription("des")
                    .setUpdatedCloudflareIntegrationParams(
                        UpdatedCloudflareIntegrationParams.newBuilder().setEmail("email")))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request, REQUEST_CONTEXT, existingWafIntegrations);
        });
  }

  @Test
  void invalidCloudflareIntegrationParamsTest() {
    CreateWafIntegrationRequest request1 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setDescription("des")
                    .setCloudflareIntegrationParams(
                        CloudflareIntegrationParams.newBuilder()
                            .setEmail("email")
                            .setApiToken("apitoken")))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request1, REQUEST_CONTEXT, existingWafIntegrations);
        });
    CreateWafIntegrationRequest request2 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setDescription("des")
                    .setCloudflareIntegrationParams(
                        CloudflareIntegrationParams.newBuilder()
                            .setZone("zone")
                            .setApiToken("apitoken")))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request2, REQUEST_CONTEXT, existingWafIntegrations);
        });
    CreateWafIntegrationRequest request3 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setDescription("des")
                    .setCloudflareIntegrationParams(
                        CloudflareIntegrationParams.newBuilder().setZone("zone").setEmail("email")))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request3, REQUEST_CONTEXT, existingWafIntegrations);
        });
  }

  @Test
  void invalidRequestsWithMissingIdsTest() {
    GetWafIntegrationRequest getRequest = GetWafIntegrationRequest.newBuilder().build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(getRequest, REQUEST_CONTEXT);
        });
    UpdateWafIntegrationRequest updateRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setDescription("des")
                    .setUpdatedCloudflareIntegrationParams(
                        UpdatedCloudflareIntegrationParams.newBuilder()
                            .setZone("zone")
                            .setEmail("email")
                            .setApiToken("apitoken")))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              updateRequest, REQUEST_CONTEXT, existingWafIntegrations);
        });
    DeleteWafIntegrationRequest deleteRequest = DeleteWafIntegrationRequest.newBuilder().build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(deleteRequest, REQUEST_CONTEXT);
        });
  }

  @Test
  void invalidCreateImpervaWafIntegrationTest() {

    ImpervaIntegrationParams.Builder impervaIntegrationParamsBuilder =
        ImpervaIntegrationParams.newBuilder()
            .setApiId("access-key")
            .setApiKey(
                EncryptedText.newBuilder().setKeyId("secret-id").setValue("secret-value").build());

    // name field is absent
    CreateWafIntegrationRequest request1 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setImpervaIntegrationParams(impervaIntegrationParamsBuilder))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request1, REQUEST_CONTEXT, existingWafIntegrations));

    // access id field is absent
    CreateWafIntegrationRequest request2 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setImpervaIntegrationParams(impervaIntegrationParamsBuilder.clearApiId()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request2, REQUEST_CONTEXT, existingWafIntegrations));

    // api key field is absent
    CreateWafIntegrationRequest request3 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setImpervaIntegrationParams(impervaIntegrationParamsBuilder.clearApiKey()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request3, REQUEST_CONTEXT, existingWafIntegrations));

    // invalid request - same name as that of an existing imperva waf integration
    WafIntegration existingImpervaWafIntegration = getExistingImpervaWafIntegration();
    CreateWafIntegrationRequest request4 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setImpervaIntegrationParams(impervaIntegrationParamsBuilder.build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request4, REQUEST_CONTEXT, List.of(existingImpervaWafIntegration)));

    // valid request - both account_id and website_params are absent
    CreateWafIntegrationRequest validRequest1 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setImpervaIntegrationParams(
                        ImpervaIntegrationParams.newBuilder()
                            .setApiId("id")
                            .setApiKey(
                                EncryptedText.newBuilder()
                                    .setKeyId("secret-id")
                                    .setValue("secret-value")
                                    .build())
                            .build()))
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest1, REQUEST_CONTEXT, existingWafIntegrations));

    // valid request - only the account_id is absent
    CreateWafIntegrationRequest validRequest2 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setImpervaIntegrationParams(
                        ImpervaIntegrationParams.newBuilder()
                            .setApiId("id")
                            .setApiKey(
                                EncryptedText.newBuilder()
                                    .setKeyId("secret-id")
                                    .setValue("secret-value")
                                    .build())
                            .setWebsiteNames(
                                StringList.newBuilder()
                                    .addValues("website1.com")
                                    .addValues("website2.com")
                                    .build())
                            .build())
                    .build())
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest2, REQUEST_CONTEXT, existingWafIntegrations));

    // valid request - website_params is absent
    CreateWafIntegrationRequest validRequest3 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setImpervaIntegrationParams(
                        ImpervaIntegrationParams.newBuilder()
                            .setApiId("id")
                            .setApiKey(
                                EncryptedText.newBuilder()
                                    .setKeyId("secret-id")
                                    .setValue("secret-value")
                                    .build())
                            .setAccountId("account-id")
                            .build())
                    .build())
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest3, REQUEST_CONTEXT, existingWafIntegrations));

    // valid request - all the parameters are present
    CreateWafIntegrationRequest validRequest4 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setImpervaIntegrationParams(
                        ImpervaIntegrationParams.newBuilder()
                            .setApiId("id")
                            .setApiKey(
                                EncryptedText.newBuilder()
                                    .setKeyId("secret-id")
                                    .setValue("secret-value")
                                    .build())
                            .setAccountId("imperva-account-id")
                            .setWebsiteNames(
                                StringList.newBuilder()
                                    .addValues("website1.com")
                                    .addValues("website2.com")
                                    .build())
                            .build())
                    .build())
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest4, REQUEST_CONTEXT, existingWafIntegrations));
  }

  @Test
  void invalidUpdateImpervaWafIntegrationTest() {
    ImpervaIntegrationUpdateParams.Builder impervaIntegrationUpdateParams =
        ImpervaIntegrationUpdateParams.newBuilder()
            .setApiId("api-id")
            .setApiKey(
                EncryptedText.newBuilder().setKeyId("secret-id").setValue("secret-value").build());

    // id field is absent
    UpdateWafIntegrationRequest request1 =
        UpdateWafIntegrationRequest.newBuilder()
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setDescription("desc")
                    .setName("name")
                    .setUpdatedImpervaIntegrationParams(impervaIntegrationUpdateParams))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request1, REQUEST_CONTEXT, existingWafIntegrations));

    // name field is absent
    UpdateWafIntegrationRequest request2 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setUpdatedImpervaIntegrationParams(impervaIntegrationUpdateParams))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request2, REQUEST_CONTEXT, existingWafIntegrations));

    // api id is absent
    UpdateWafIntegrationRequest request4 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedImpervaIntegrationParams(
                        impervaIntegrationUpdateParams.clearApiId()))
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request4, REQUEST_CONTEXT, existingWafIntegrations));

    // api id is empty
    UpdateWafIntegrationRequest request5 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedImpervaIntegrationParams(
                        impervaIntegrationUpdateParams.setApiId("")))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request5, REQUEST_CONTEXT, existingWafIntegrations));

    // api key is absent
    UpdateWafIntegrationRequest request6 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedImpervaIntegrationParams(
                        ImpervaIntegrationUpdateParams.newBuilder().setApiId("fsdkj").build()))
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request6, REQUEST_CONTEXT, existingWafIntegrations));

    // api key value is absent
    UpdateWafIntegrationRequest request7 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedImpervaIntegrationParams(
                        impervaIntegrationUpdateParams.setApiKey(
                            EncryptedText.newBuilder().setValue("secret-value"))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request7, REQUEST_CONTEXT, existingWafIntegrations));

    // api key id is absent
    UpdateWafIntegrationRequest request8 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedImpervaIntegrationParams(
                        impervaIntegrationUpdateParams.setApiKey(
                            EncryptedText.newBuilder().setKeyId("secret-id"))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request8, REQUEST_CONTEXT, existingWafIntegrations));

    // invalid request - same name as that of an already existing imperva waf integration
    WafIntegration existingImpervaWafIntegration = getExistingImpervaWafIntegration();
    UpdateWafIntegrationRequest request9 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedImpervaIntegrationParams(impervaIntegrationUpdateParams)
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request9, REQUEST_CONTEXT, List.of(existingImpervaWafIntegration)));

    // valid request - both account_id and website_params are absent
    UpdateWafIntegrationRequest validRequest1 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name1")
                    .setUpdatedImpervaIntegrationParams(
                        ImpervaIntegrationUpdateParams.newBuilder()
                            .setApiId("api-id")
                            .setApiKey(
                                EncryptedText.newBuilder()
                                    .setKeyId("secret-id1")
                                    .setValue("secret-value1")
                                    .build())
                            .build()))
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest1, REQUEST_CONTEXT, existingWafIntegrations));

    // valid request - website_params is absent
    UpdateWafIntegrationRequest validRequest2 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name1")
                    .setUpdatedImpervaIntegrationParams(
                        ImpervaIntegrationUpdateParams.newBuilder()
                            .setApiId("api-id1")
                            .setApiKey(
                                EncryptedText.newBuilder()
                                    .setKeyId("secret-id1")
                                    .setValue("secret-value1")
                                    .build())
                            .setAccountId("account-id1")
                            .build())
                    .build())
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest2, REQUEST_CONTEXT, existingWafIntegrations));

    // valid request - account_id is absent
    UpdateWafIntegrationRequest validRequest3 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name1")
                    .setUpdatedImpervaIntegrationParams(
                        ImpervaIntegrationUpdateParams.newBuilder()
                            .setApiId("api-id1")
                            .setApiKey(
                                EncryptedText.newBuilder()
                                    .setKeyId("secret-id1")
                                    .setValue("secret-value1")
                                    .build())
                            .setWebsiteNames(
                                StringList.newBuilder()
                                    .addValues("website1.com")
                                    .addValues("website2.com")
                                    .build())
                            .build())
                    .build())
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest3, REQUEST_CONTEXT, existingWafIntegrations));

    // valid request - all parameters are present
    UpdateWafIntegrationRequest validRequest4 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name1")
                    .setUpdatedImpervaIntegrationParams(
                        ImpervaIntegrationUpdateParams.newBuilder()
                            .setApiId("api-id1")
                            .setApiKey(
                                EncryptedText.newBuilder()
                                    .setKeyId("secret-id1")
                                    .setValue("secret-value1")
                                    .build())
                            .setAccountId("account-id1")
                            .setWebsiteNames(
                                StringList.newBuilder()
                                    .addValues("website1.com")
                                    .addValues("website2.com")
                                    .build())
                            .build())
                    .build())
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest4, REQUEST_CONTEXT, existingWafIntegrations));
  }

  @Test
  void invalidCreateAwsWafIntegrationTest() {
    AwsIntegrationParams.Builder awsIntegrationParamsBuilder =
        AwsIntegrationParams.newBuilder()
            .setAuthCredentials(
                AuthCredentials.newBuilder()
                    .setAccessKeyId("access-key")
                    .setEncryptedSecretAccessKey("secret")
                    .build())
            .addAllResources(
                List.of(AwsResource.newBuilder().setArn("arn").setRegion("region").build()));

    // name field is absent
    CreateWafIntegrationRequest request1 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setAwsIntegrationParams(awsIntegrationParamsBuilder))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request1, REQUEST_CONTEXT, existingWafIntegrations);
        });

    // access key id field is absent
    CreateWafIntegrationRequest request2 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAwsIntegrationParams(
                        awsIntegrationParamsBuilder
                            .clearAuthCredentials()
                            .setAuthCredentials(
                                AuthCredentials.newBuilder()
                                    .setEncryptedSecretAccessKey("secret"))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request2, REQUEST_CONTEXT, existingWafIntegrations);
        });

    // secret field is absent
    CreateWafIntegrationRequest request3 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAwsIntegrationParams(
                        awsIntegrationParamsBuilder
                            .clearAuthCredentials()
                            .setAuthCredentials(
                                AuthCredentials.newBuilder().setAccessKeyId("access-key"))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request3, REQUEST_CONTEXT, existingWafIntegrations);
        });

    // resources field is absent
    CreateWafIntegrationRequest request4 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAwsIntegrationParams(awsIntegrationParamsBuilder.clearResources()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request4, REQUEST_CONTEXT, existingWafIntegrations);
        });

    WafIntegration wafIntegration1 =
        WafIntegration.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("existingName")
                    .setAwsIntegrationParams(
                        AwsIntegrationParams.newBuilder()
                            .setWebIdentityAuthCredentials(
                                WebIdentityAuthenticationCredentials.newBuilder()
                                    .setRoleArn("arn:aws:398429084503/role"))
                            .addResources(
                                AwsResource.newBuilder().setArn("arn1").setRegion("region"))))
            .build();
    CreateWafIntegrationRequest validRequest =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAwsIntegrationParams(
                        AwsIntegrationParams.newBuilder()
                            .setWebIdentityAuthCredentials(
                                WebIdentityAuthenticationCredentials.newBuilder()
                                    .setRoleArn("arn:aws:398429084503/role"))
                            .addResources(
                                AwsResource.newBuilder().setArn("arn").setRegion("region"))))
            .build();

    // resource present but invalid
    testWithInvalidAwsResource(validRequest);

    // valid request
    assertDoesNotThrow(
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              validRequest, REQUEST_CONTEXT, List.of(wafIntegration1));
        });

    // role arn absent for web identity authentication
    assertThrows(
        RuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              CreateWafIntegrationRequest.newBuilder()
                  .setWafIntegrationDetails(
                      WafIntegrationDetails.newBuilder()
                          .setName("name")
                          .setAwsIntegrationParams(
                              AwsIntegrationParams.newBuilder()
                                  .setWebIdentityAuthCredentials(
                                      WebIdentityAuthenticationCredentials.newBuilder()
                                          .setRoleArn(""))
                                  .addResources(
                                      AwsResource.newBuilder().setArn("arn").setRegion("region"))))
                  .build(),
              REQUEST_CONTEXT,
              existingWafIntegrations);
        });
    // aws waf integration already exist with same arn

    WafIntegrationDetails wafIntegrationDetail =
        WafIntegrationDetails.newBuilder()
            .setName("name")
            .setAwsIntegrationParams(
                AwsIntegrationParams.newBuilder()
                    .setWebIdentityAuthCredentials(
                        WebIdentityAuthenticationCredentials.newBuilder()
                            .setRoleArn("arn:aws:398429084503/role"))
                    .addResources(AwsResource.newBuilder().setArn("arn").setRegion("region")))
            .build();
    CreateWafIntegrationRequest request5 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(wafIntegrationDetail)
            .build();
    WafIntegration wafIntegration =
        WafIntegration.newBuilder().setWafIntegrationDetails(wafIntegrationDetail).build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request5, REQUEST_CONTEXT, List.of(wafIntegration));
        });
  }

  @Test
  void invalidUpdateAwsRequestTest() {
    AwsIntegrationUpdateParams.Builder awsIntegrationUpdateParams =
        AwsIntegrationUpdateParams.newBuilder()
            .setAuthCredentials(AuthCredentials.newBuilder().setAccessKeyId("access-key"))
            .addAllResources(
                List.of(AwsResource.newBuilder().setArn("arn").setRegion("region").build()));

    // id field is absent
    UpdateWafIntegrationRequest request1 =
        UpdateWafIntegrationRequest.newBuilder()
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setDescription("desc")
                    .setName("name")
                    .setUpdatedAwsIntegrationParams(awsIntegrationUpdateParams))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request1, REQUEST_CONTEXT, existingWafIntegrations);
        });

    // name field is absent
    UpdateWafIntegrationRequest request2 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setUpdatedAwsIntegrationParams(awsIntegrationUpdateParams))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request2, REQUEST_CONTEXT, existingWafIntegrations);
        });

    // resources field is absent
    UpdateWafIntegrationRequest request4 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedAwsIntegrationParams(awsIntegrationUpdateParams.clearResources()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request4, REQUEST_CONTEXT, existingWafIntegrations);
        });

    // empty environment id
    UpdateWafIntegrationRequest request5 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setWafIntegrationScope(
                        WafIntegrationScope.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of())))
                    .setUpdatedAwsIntegrationParams(
                        AwsIntegrationUpdateParams.newBuilder()
                            .setAuthCredentials(
                                AuthCredentials.newBuilder()
                                    .setAccessKeyId("id")
                                    .setEncryptedSecretAccessKey("key"))
                            .addResources(
                                AwsResource.newBuilder().setArn("arn").setRegion("region"))))
            .build();
    WafIntegration wafIntegration =
        WafIntegration.newBuilder()
            .setId("id1")
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("existingName")
                    .setAwsIntegrationParams(
                        AwsIntegrationParams.newBuilder()
                            .setWebIdentityAuthCredentials(
                                WebIdentityAuthenticationCredentials.newBuilder()
                                    .setRoleArn("arn:aws:398429084503/role"))
                            .addResources(
                                AwsResource.newBuilder().setArn("arn1").setRegion("region"))))
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request5, REQUEST_CONTEXT, List.of(wafIntegration)));

    UpdateWafIntegrationRequest validRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedAwsIntegrationParams(
                        AwsIntegrationUpdateParams.newBuilder()
                            .setAuthCredentials(
                                AuthCredentials.newBuilder()
                                    .setAccessKeyId("id")
                                    .setEncryptedSecretAccessKey("key"))
                            .addResources(
                                AwsResource.newBuilder().setArn("arn").setRegion("region"))))
            .build();

    // resource present but invalid
    testWithInvalidAwsResource(validRequest);

    // valid request
    assertDoesNotThrow(
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              validRequest, REQUEST_CONTEXT, existingWafIntegrations);
        });

    // updating integration with same id can have same arn
    WafIntegrationDetails wafIntegrationDetails =
        WafIntegrationDetails.newBuilder()
            .setName("name")
            .setAwsIntegrationParams(
                AwsIntegrationParams.newBuilder()
                    .setWebIdentityAuthCredentials(
                        WebIdentityAuthenticationCredentials.newBuilder()
                            .setRoleArn("arn:aws:398429084503/role"))
                    .addResources(AwsResource.newBuilder().setArn("arn").setRegion("region")))
            .build();
    WafIntegration wafIntegration1 =
        WafIntegration.newBuilder()
            .setId("id")
            .setWafIntegrationDetails(wafIntegrationDetails)
            .build();
    UpdateWafIntegrationRequest request6 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("existingName")
                    .setUpdatedAwsIntegrationParams(
                        AwsIntegrationUpdateParams.newBuilder()
                            .setAuthCredentials(
                                AuthCredentials.newBuilder()
                                    .setAccessKeyId("id")
                                    .setEncryptedSecretAccessKey("key"))
                            .addResources(
                                AwsResource.newBuilder().setArn("arn").setRegion("region"))))
            .build();
    assertDoesNotThrow(
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request6, REQUEST_CONTEXT, List.of(wafIntegration1));
        });

    // aws waf integration already exist with same arn.
    WafIntegration wafIntegration2 =
        WafIntegration.newBuilder()
            .setId("id1")
            .setWafIntegrationDetails(wafIntegrationDetails)
            .build();
    UpdateWafIntegrationRequest request =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedAwsIntegrationParams(
                        AwsIntegrationUpdateParams.newBuilder()
                            .setAuthCredentials(
                                AuthCredentials.newBuilder()
                                    .setAccessKeyId("id")
                                    .setEncryptedSecretAccessKey("key"))
                            .addResources(
                                AwsResource.newBuilder().setArn("arn").setRegion("region"))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request, REQUEST_CONTEXT, List.of(wafIntegration2));
        });
  }

  @Test
  void invalidCreateAzureRequestTest() {

    // empty azure integration params list
    CreateWafIntegrationRequest request1 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setAzureIntegrationParams(AzureIntegrationParams.getDefaultInstance()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request1, REQUEST_CONTEXT, existingWafIntegrations));

    // missing azure tenant-id
    CreateWafIntegrationRequest request2 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setSubscriptionId(SUBSCRIPTION_ID)
                                    .setAzureEnvironment(AZURE_ENVIRONMENT)
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyName(WAF_POLICY)
                                            .setWafPolicyResourceGroupName(
                                                WAF_POLICY_RESOURCE_GROUP)
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId(CLIENT_ID)
                                            .setEncryptedClientSecret(CLIENT_SECRET)
                                            .setAccessKeyId(ACCESS_KEY)))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request2, REQUEST_CONTEXT, existingWafIntegrations));

    // missing azure subscription-id
    CreateWafIntegrationRequest request3 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId(TENANT_ID)
                                    .setAzureEnvironment(AZURE_ENVIRONMENT)
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyName(WAF_POLICY)
                                            .setWafPolicyResourceGroupName(
                                                WAF_POLICY_RESOURCE_GROUP)
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId(CLIENT_ID)
                                            .setEncryptedClientSecret(CLIENT_SECRET)
                                            .setAccessKeyId(ACCESS_KEY)))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request3, REQUEST_CONTEXT, existingWafIntegrations));

    // missing azure env
    CreateWafIntegrationRequest request4 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId(TENANT_ID)
                                    .setSubscriptionId(SUBSCRIPTION_ID)
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyName(WAF_POLICY)
                                            .setWafPolicyResourceGroupName(
                                                WAF_POLICY_RESOURCE_GROUP)
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId(CLIENT_ID)
                                            .setEncryptedClientSecret(CLIENT_SECRET)
                                            .setAccessKeyId(ACCESS_KEY)))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request4, REQUEST_CONTEXT, existingWafIntegrations));

    // missing azure client-id
    CreateWafIntegrationRequest request8 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId(TENANT_ID)
                                    .setSubscriptionId(SUBSCRIPTION_ID)
                                    .setAzureEnvironment(AZURE_ENVIRONMENT)
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyName(WAF_POLICY)
                                            .setWafPolicyResourceGroupName(
                                                WAF_POLICY_RESOURCE_GROUP)
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setEncryptedClientSecret(CLIENT_SECRET)
                                            .setAccessKeyId(ACCESS_KEY)))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request8, REQUEST_CONTEXT, existingWafIntegrations));

    // missing azure client secret
    CreateWafIntegrationRequest request9 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId(TENANT_ID)
                                    .setSubscriptionId(SUBSCRIPTION_ID)
                                    .setAzureEnvironment(AZURE_ENVIRONMENT)
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyName(WAF_POLICY)
                                            .setWafPolicyResourceGroupName(
                                                WAF_POLICY_RESOURCE_GROUP)
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId(CLIENT_ID)
                                            .setAccessKeyId(ACCESS_KEY)))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request9, REQUEST_CONTEXT, existingWafIntegrations));

    // missing azure access key id
    CreateWafIntegrationRequest request10 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId(TENANT_ID)
                                    .setSubscriptionId(SUBSCRIPTION_ID)
                                    .setAzureEnvironment(AZURE_ENVIRONMENT)
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyName(WAF_POLICY)
                                            .setWafPolicyResourceGroupName(
                                                WAF_POLICY_RESOURCE_GROUP)
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId(CLIENT_ID)
                                            .setEncryptedClientSecret(CLIENT_SECRET)))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request10, REQUEST_CONTEXT, existingWafIntegrations));

    // missing waf policy name
    CreateWafIntegrationRequest request11 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setWafIntegrationScope(
                        WafIntegrationScope.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of())))
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId(TENANT_ID)
                                    .setSubscriptionId(SUBSCRIPTION_ID)
                                    .setAzureEnvironment(AZURE_ENVIRONMENT)
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyResourceGroupName(
                                                WAF_POLICY_RESOURCE_GROUP)
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId(CLIENT_ID)
                                            .setEncryptedClientSecret(CLIENT_SECRET)
                                            .setAccessKeyId(ACCESS_KEY)))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request11, REQUEST_CONTEXT, existingWafIntegrations));

    // missing waf policy rg name
    CreateWafIntegrationRequest request12 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setWafIntegrationScope(
                        WafIntegrationScope.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of())))
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId(TENANT_ID)
                                    .setSubscriptionId(SUBSCRIPTION_ID)
                                    .setAzureEnvironment(AZURE_ENVIRONMENT)
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyName(WAF_POLICY)
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId(CLIENT_ID)
                                            .setEncryptedClientSecret(CLIENT_SECRET)
                                            .setAccessKeyId(ACCESS_KEY)))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request12, REQUEST_CONTEXT, existingWafIntegrations));

    // policy type not set
    CreateWafIntegrationRequest request13 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setWafIntegrationScope(
                        WafIntegrationScope.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of())))
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId(TENANT_ID)
                                    .setSubscriptionId(SUBSCRIPTION_ID)
                                    .setAzureEnvironment(AZURE_ENVIRONMENT)
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyName(WAF_POLICY)
                                            .setWafPolicyResourceGroupName(
                                                WAF_POLICY_RESOURCE_GROUP))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId(CLIENT_ID)
                                            .setEncryptedClientSecret(CLIENT_SECRET)
                                            .setAccessKeyId(ACCESS_KEY)))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request13, REQUEST_CONTEXT, existingWafIntegrations));

    // getting an existing azure waf integration to check for invalid cases of name-match or
    // match of a combination of tenantId, resourceGroup and policyType
    WafIntegration existingAzureWafIntegration = getExistingAzureWafIntegration();

    // invalid request - create azure integration of same name
    CreateWafIntegrationRequest request14 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setWafIntegrationScope(
                        WafIntegrationScope.newBuilder()
                            .setEnvironmentScope(EnvironmentScope.getDefaultInstance()))
                    .setAzureIntegrationParams(getAzureIntegrationParams()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request14, REQUEST_CONTEXT, List.of(existingAzureWafIntegration)));

    // invalid request - create azure integration having the same values of tenantId, resourceGroup
    // and policyName
    CreateWafIntegrationRequest request15 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setWafIntegrationScope(
                        WafIntegrationScope.newBuilder()
                            .setEnvironmentScope(EnvironmentScope.getDefaultInstance()))
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId(TENANT_ID)
                                    .setSubscriptionId(SUBSCRIPTION_ID)
                                    .setAzureEnvironment(AZURE_ENVIRONMENT)
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId(CLIENT_ID)
                                            .setEncryptedClientSecret(CLIENT_SECRET)
                                            .setAccessKeyId(ACCESS_KEY)
                                            .build())
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyName(WAF_POLICY)
                                            .setWafPolicyResourceGroupName(
                                                WAF_POLICY_RESOURCE_GROUP)
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY)
                                            .build())
                                    .build())))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request15, REQUEST_CONTEXT, List.of(existingAzureWafIntegration)));

    // valid request - 1 out of the 3 values of tenantId, policyResourceGroup or wafPolicyName is
    // different
    CreateWafIntegrationRequest validRequest1 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setWafIntegrationScope(
                        WafIntegrationScope.newBuilder()
                            .setEnvironmentScope(EnvironmentScope.getDefaultInstance()))
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId(TENANT_ID)
                                    .setSubscriptionId(SUBSCRIPTION_ID)
                                    .setAzureEnvironment(AZURE_ENVIRONMENT)
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId(CLIENT_ID)
                                            .setEncryptedClientSecret(CLIENT_SECRET)
                                            .setAccessKeyId(ACCESS_KEY)
                                            .build())
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyName(WAF_POLICY + "1")
                                            .setWafPolicyResourceGroupName(
                                                WAF_POLICY_RESOURCE_GROUP)
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType.AZURE_WAF_POLICY_TYPE_FRONT_DOOR)
                                            .build())
                                    .build())))
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest1, REQUEST_CONTEXT, List.of(existingAzureWafIntegration)));

    // valid request
    CreateWafIntegrationRequest validRequest2 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setWafIntegrationScope(
                        WafIntegrationScope.newBuilder()
                            .setEnvironmentScope(EnvironmentScope.getDefaultInstance()))
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId(TENANT_ID)
                                    .setSubscriptionId(SUBSCRIPTION_ID)
                                    .setAzureEnvironment(AZURE_ENVIRONMENT)
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyName(WAF_POLICY)
                                            .setWafPolicyResourceGroupName(
                                                WAF_POLICY_RESOURCE_GROUP)
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId(CLIENT_ID)
                                            .setEncryptedClientSecret(CLIENT_SECRET)
                                            .setAccessKeyId(ACCESS_KEY)))))
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest2, REQUEST_CONTEXT, existingWafIntegrations));
  }

  @Test
  void invalidCreateGcpRequestTest() {
    // empty gcp integration details
    CreateWafIntegrationRequest request1 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setGcpIntegrationParams(GcpIntegrationParams.getDefaultInstance()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request1, REQUEST_CONTEXT, existingWafIntegrations));

    // missing gcp project-id
    CreateWafIntegrationRequest request2 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setGcpIntegrationParams(
                        GcpIntegrationParams.newBuilder()
                            .setGcpIntegrationDetails(
                                GcpIntegrationDetails.newBuilder()
                                    .setSecurityPolicyName("policy-1")
                                    .setDenyActionResponseCodeValue(502)
                                    .setAuthCredentials(
                                        GcpAuthCredentials.newBuilder()
                                            .setEncryptedServiceAccountKey(
                                                GcpAuthCredentials.EncryptedText.newBuilder()
                                                    .setValue("secret")
                                                    .setKeyId("key-id")
                                                    .build()))
                                    .setGlobalSecurityPolicyScope(
                                        GlobalSecurityPolicyScope.getDefaultInstance()))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request2, REQUEST_CONTEXT, existingWafIntegrations));

    // missing gcp security policy
    CreateWafIntegrationRequest request3 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setGcpIntegrationParams(
                        GcpIntegrationParams.newBuilder()
                            .setGcpIntegrationDetails(
                                GcpIntegrationDetails.newBuilder()
                                    .setProjectId("project-1")
                                    .setDenyActionResponseCodeValue(502)
                                    .setAuthCredentials(
                                        GcpAuthCredentials.newBuilder()
                                            .setEncryptedServiceAccountKey(
                                                GcpAuthCredentials.EncryptedText.newBuilder()
                                                    .setValue("secret")
                                                    .setKeyId("key-id")
                                                    .build()))
                                    .setGlobalSecurityPolicyScope(
                                        GlobalSecurityPolicyScope.getDefaultInstance()))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request3, REQUEST_CONTEXT, existingWafIntegrations));

    // missing gcp service account key
    CreateWafIntegrationRequest request4 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setGcpIntegrationParams(
                        GcpIntegrationParams.newBuilder()
                            .setGcpIntegrationDetails(
                                GcpIntegrationDetails.newBuilder()
                                    .setProjectId("project-1")
                                    .setSecurityPolicyName("policy-1")
                                    .setDenyActionResponseCodeValue(502)
                                    .setAuthCredentials(
                                        GcpAuthCredentials.newBuilder()
                                            .setEncryptedServiceAccountKey(
                                                GcpAuthCredentials.EncryptedText.newBuilder()
                                                    .setKeyId("key-id")
                                                    .build()))
                                    .setGlobalSecurityPolicyScope(
                                        GlobalSecurityPolicyScope.getDefaultInstance()))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request4, REQUEST_CONTEXT, existingWafIntegrations));

    // missing gcp key id
    CreateWafIntegrationRequest request5 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setGcpIntegrationParams(
                        GcpIntegrationParams.newBuilder()
                            .setGcpIntegrationDetails(
                                GcpIntegrationDetails.newBuilder()
                                    .setProjectId("project-1")
                                    .setSecurityPolicyName("policy-1")
                                    .setDenyActionResponseCodeValue(502)
                                    .setAuthCredentials(
                                        GcpAuthCredentials.newBuilder()
                                            .setEncryptedServiceAccountKey(
                                                GcpAuthCredentials.EncryptedText.newBuilder()
                                                    .setValue("secret")
                                                    .build()))
                                    .setGlobalSecurityPolicyScope(
                                        GlobalSecurityPolicyScope.getDefaultInstance()))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request5, REQUEST_CONTEXT, existingWafIntegrations));

    // region security policy set but region field is missing
    CreateWafIntegrationRequest request6 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setGcpIntegrationParams(
                        GcpIntegrationParams.newBuilder()
                            .setGcpIntegrationDetails(
                                GcpIntegrationDetails.newBuilder()
                                    .setProjectId("project-1")
                                    .setSecurityPolicyName("policy-1")
                                    .setDenyActionResponseCodeValue(502)
                                    .setAuthCredentials(
                                        GcpAuthCredentials.newBuilder()
                                            .setEncryptedServiceAccountKey(
                                                GcpAuthCredentials.EncryptedText.newBuilder()
                                                    .setValue("secret")
                                                    .setKeyId("key-id")
                                                    .build()))
                                    .setRegionSecurityPolicyScope(
                                        RegionSecurityPolicyScope.getDefaultInstance()))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request6, REQUEST_CONTEXT, existingWafIntegrations));

    // valid request
    CreateWafIntegrationRequest validRequest =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setGcpIntegrationParams(
                        GcpIntegrationParams.newBuilder()
                            .setGcpIntegrationDetails(
                                GcpIntegrationDetails.newBuilder()
                                    .setProjectId("project-1")
                                    .setSecurityPolicyName("policy-1")
                                    .setDenyActionResponseCodeValue(502)
                                    .setAuthCredentials(
                                        GcpAuthCredentials.newBuilder()
                                            .setEncryptedServiceAccountKey(
                                                GcpAuthCredentials.EncryptedText.newBuilder()
                                                    .setValue("secret")
                                                    .setKeyId("key-id")
                                                    .build()))
                                    .setGlobalSecurityPolicyScope(
                                        GlobalSecurityPolicyScope.getDefaultInstance()))))
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest, REQUEST_CONTEXT, existingWafIntegrations));

    // request containing an existing security policy name and project should throw
    WafIntegration existingGcpWafIntegration = getExistingGcpWafIntegration();

    CreateWafIntegrationRequest invalidRequest =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setGcpIntegrationParams(
                        GcpIntegrationParams.newBuilder()
                            .setGcpIntegrationDetails(
                                GcpIntegrationDetails.newBuilder()
                                    .setProjectId(EXISTING_POLICY_ID)
                                    .setSecurityPolicyName(EXISTING_SECURITY_POLICY_NAME)
                                    .setDenyActionResponseCodeValue(502)
                                    .setAuthCredentials(
                                        GcpAuthCredentials.newBuilder()
                                            .setEncryptedServiceAccountKey(
                                                GcpAuthCredentials.EncryptedText.newBuilder()
                                                    .setValue("secret")
                                                    .setKeyId("key-id")
                                                    .build()))
                                    .setGlobalSecurityPolicyScope(
                                        GlobalSecurityPolicyScope.getDefaultInstance()))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest, REQUEST_CONTEXT, List.of(existingGcpWafIntegration)));
  }

  @Test
  void testInvalidCreateF5RequestTest() {
    // empty f5 integration details
    CreateWafIntegrationRequest request1 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setF5IntegrationParams(F5IntegrationParams.getDefaultInstance()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request1, REQUEST_CONTEXT, existingWafIntegrations));

    // missing f5 url
    CreateWafIntegrationRequest request2 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setF5IntegrationParams(
                        F5IntegrationParams.newBuilder()
                            .setF5IntegrationDetails(
                                F5IntegrationDetails.newBuilder()
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
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request2, REQUEST_CONTEXT, existingWafIntegrations));

    // missing f5 security policy
    CreateWafIntegrationRequest request3 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setF5IntegrationParams(
                        F5IntegrationParams.newBuilder()
                            .setF5IntegrationDetails(
                                F5IntegrationDetails.newBuilder()
                                    .setUrl("https://localhost:9000")
                                    .setF5AuthCredentials(
                                        F5AuthCredentials.newBuilder()
                                            .setEncryptedUserName("user-name")
                                            .setEncryptedPassword("password")
                                            .setEncryptionKeyId("key-id"))))
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request3, REQUEST_CONTEXT, existingWafIntegrations));

    // missing f5 auth credentials
    CreateWafIntegrationRequest request4 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setF5IntegrationParams(
                        F5IntegrationParams.newBuilder()
                            .setF5IntegrationDetails(
                                F5IntegrationDetails.newBuilder()
                                    .setF5PolicyDetails(
                                        F5PolicyDetails.newBuilder()
                                            .setPolicyId("policy1")
                                            .setPolicyName("policyname")
                                            .build())
                                    .setUrl("https://localhost:9000")))
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request4, REQUEST_CONTEXT, existingWafIntegrations));

    // valid request
    CreateWafIntegrationRequest validRequest =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
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
                    .build())
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest, REQUEST_CONTEXT, existingWafIntegrations));

    // request creating integration for which the policy-name already exists should throw
    WafIntegration existingF5WafIntegration = getExistingF5WafIntegration();
    CreateWafIntegrationRequest invalidRequest =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setF5IntegrationParams(
                        F5IntegrationParams.newBuilder()
                            .setF5IntegrationDetails(
                                F5IntegrationDetails.newBuilder()
                                    .setUrl(EXSITING_F5_URL)
                                    .setF5PolicyDetails(
                                        F5PolicyDetails.newBuilder()
                                            .setPolicyId("policy1")
                                            .setPolicyName(EXISTING_SECURITY_POLICY_NAME)
                                            .build())
                                    .setF5AuthCredentials(
                                        F5AuthCredentials.newBuilder()
                                            .setEncryptedUserName("user-name")
                                            .setEncryptedPassword("password")
                                            .setEncryptionKeyId("key-id"))))
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest, REQUEST_CONTEXT, List.of(existingF5WafIntegration)));
  }

  @Test
  void testInvalidCreateAkamaiRequestTest() {
    // empty akamai integration details
    CreateWafIntegrationRequest request1 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAkamaiIntegrationParams(AkamaiIntegrationParams.getDefaultInstance()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request1, REQUEST_CONTEXT, existingWafIntegrations));

    // missing akamai host
    CreateWafIntegrationRequest request2 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAkamaiIntegrationParams(
                        AkamaiIntegrationParams.newBuilder()
                            .setAkamaiIntegrationDetails(
                                AkamaiIntegrationDetails.newBuilder()
                                    .setAkamaiPolicyDetails(
                                        AkamaiPolicyDetails.newBuilder()
                                            .setPolicyId("policy1")
                                            .setAkamaiPolicyConfigurationId("config1")
                                            .build())
                                    .setAkamaiAuthCredentials(
                                        AkamaiAuthCredentials.newBuilder()
                                            .setEncryptedAccessToken("access-token")
                                            .setEncryptedClientToken("client-token")
                                            .setEncryptedClientSecret("client-secret")
                                            .setEncryptionKeyId("key-id"))))
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request2, REQUEST_CONTEXT, existingWafIntegrations));

    // missing Akamai security policy
    CreateWafIntegrationRequest request3 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAkamaiIntegrationParams(
                        AkamaiIntegrationParams.newBuilder()
                            .setAkamaiIntegrationDetails(
                                AkamaiIntegrationDetails.newBuilder()
                                    .setHost("https://localhost:9000")
                                    .setAkamaiAuthCredentials(
                                        AkamaiAuthCredentials.newBuilder()
                                            .setEncryptedAccessToken("access-token")
                                            .setEncryptedClientToken("client-token")
                                            .setEncryptedClientSecret("client-secret")
                                            .setEncryptionKeyId("key-id"))))
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request3, REQUEST_CONTEXT, existingWafIntegrations));

    // missing akamai auth credentials
    CreateWafIntegrationRequest request4 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAkamaiIntegrationParams(
                        AkamaiIntegrationParams.newBuilder()
                            .setAkamaiIntegrationDetails(
                                AkamaiIntegrationDetails.newBuilder()
                                    .setHost("https://localhost:9000")))
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request4, REQUEST_CONTEXT, existingWafIntegrations));

    // valid request
    CreateWafIntegrationRequest validRequest =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAkamaiIntegrationParams(
                        AkamaiIntegrationParams.newBuilder()
                            .setAkamaiIntegrationDetails(
                                AkamaiIntegrationDetails.newBuilder()
                                    .setHost("https://localhost:9000")
                                    .setAkamaiPolicyDetails(
                                        AkamaiPolicyDetails.newBuilder()
                                            .setPolicyId("policy1")
                                            .setAkamaiPolicyConfigurationId("config1")
                                            .build())
                                    .setAkamaiAuthCredentials(
                                        AkamaiAuthCredentials.newBuilder()
                                            .setEncryptedAccessToken("access-token")
                                            .setEncryptedClientToken("client-token")
                                            .setEncryptedClientSecret("client-secret")
                                            .setEncryptionKeyId("key-id"))))
                    .build())
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest, REQUEST_CONTEXT, existingWafIntegrations));

    // request creating integration for which the policy-name already exists should throw
    WafIntegration existingF5WafIntegration = getExistingAkamaiWafIntegration();
    CreateWafIntegrationRequest invalidRequest =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAkamaiIntegrationParams(
                        AkamaiIntegrationParams.newBuilder()
                            .setAkamaiIntegrationDetails(
                                AkamaiIntegrationDetails.newBuilder()
                                    .setHost("host")
                                    .setAkamaiPolicyDetails(
                                        AkamaiPolicyDetails.newBuilder()
                                            .setPolicyId(EXISTING_POLICY_ID)
                                            .setAkamaiPolicyConfigurationId("config1")
                                            .build())
                                    .setAkamaiAuthCredentials(
                                        AkamaiAuthCredentials.newBuilder()
                                            .setEncryptedAccessToken("access-token")
                                            .setEncryptedClientToken("client-token")
                                            .setEncryptedClientSecret("client-secret")
                                            .setEncryptionKeyId("key-id"))))
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest, REQUEST_CONTEXT, List.of(existingF5WafIntegration)));
  }

  @Test
  void invalidCreateFortinetRequestTest() {

    // empty FortinetIntegrationParams
    CreateWafIntegrationRequest invalidRequest1 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .clearFortinetIntegrationParams()
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest1, REQUEST_CONTEXT, existingWafIntegrations));

    // empty FortinetIntegrationDetails
    CreateWafIntegrationRequest invalidRequest2 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setFortinetIntegrationParams(
                        FortinetIntegrationParams.newBuilder()
                            .clearFortinetIntegrationDetails()
                            .build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest2, REQUEST_CONTEXT, existingWafIntegrations));

    // empty FortinetAuthCredentials, but with FortinetApplicationRuleDetails
    CreateWafIntegrationRequest invalidRequest3 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setFortinetIntegrationParams(
                        FortinetIntegrationParams.newBuilder()
                            .setFortinetIntegrationDetails(
                                FortinetIntegrationDetails.newBuilder()
                                    .clearFortinetAuthCredentials()
                                    .setFortinetRuleDetails(
                                        FortinetRuleDetails.newBuilder()
                                            .setFortinetApplication(
                                                getFortinetApplicationRuleDetails())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest3, REQUEST_CONTEXT, existingWafIntegrations));

    // empty FortinetAuthCredentials, but with FortinetTemplateRuleDetails
    CreateWafIntegrationRequest invalidRequest4 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setFortinetIntegrationParams(
                        FortinetIntegrationParams.newBuilder()
                            .setFortinetIntegrationDetails(
                                FortinetIntegrationDetails.newBuilder()
                                    .clearFortinetAuthCredentials()
                                    .setFortinetRuleDetails(
                                        FortinetRuleDetails.newBuilder()
                                            .setFortinetTemplate(getFortinetTemplateRuleDetails())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest4, REQUEST_CONTEXT, existingWafIntegrations));

    // empty FortinetApplicationRuleDetails, but with FortinetAuthCredentials
    CreateWafIntegrationRequest invalidRequest5 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setFortinetIntegrationParams(
                        FortinetIntegrationParams.newBuilder()
                            .setFortinetIntegrationDetails(
                                FortinetIntegrationDetails.newBuilder()
                                    .setFortinetAuthCredentials(getFortinetAuthCredentials())
                                    .setFortinetRuleDetails(
                                        FortinetRuleDetails.newBuilder()
                                            .clearFortinetApplication()
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest5, REQUEST_CONTEXT, existingWafIntegrations));

    // empty FortinetTemplateRuleDetails, but with FortinetAuthCredentials
    CreateWafIntegrationRequest invalidRequest6 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setFortinetIntegrationParams(
                        FortinetIntegrationParams.newBuilder()
                            .setFortinetIntegrationDetails(
                                FortinetIntegrationDetails.newBuilder()
                                    .setFortinetAuthCredentials(getFortinetAuthCredentials())
                                    .setFortinetRuleDetails(
                                        FortinetRuleDetails.newBuilder()
                                            .clearFortinetTemplate()
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest6, REQUEST_CONTEXT, existingWafIntegrations));

    // same name as that of an already existing fortinet waf integration
    WafIntegration existingFortinetWafIntegration = getExistingFortinetWafIntegration();
    CreateWafIntegrationRequest invalidRequest7 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setFortinetIntegrationParams(
                        FortinetIntegrationParams.newBuilder()
                            .setFortinetIntegrationDetails(getFortinetIntegrationDetails())
                            .build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest7, REQUEST_CONTEXT, List.of(existingFortinetWafIntegration)));

    // valid request 1
    CreateWafIntegrationRequest validRequest1 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setFortinetIntegrationParams(
                        FortinetIntegrationParams.newBuilder()
                            .setFortinetIntegrationDetails(
                                FortinetIntegrationDetails.newBuilder()
                                    .setFortinetAuthCredentials(getFortinetAuthCredentials())
                                    .setFortinetRuleDetails(
                                        FortinetRuleDetails.newBuilder()
                                            .setFortinetApplication(
                                                getFortinetApplicationRuleDetails())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest1, REQUEST_CONTEXT, existingWafIntegrations));

    // valid request 2
    CreateWafIntegrationRequest validRequest2 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setFortinetIntegrationParams(
                        FortinetIntegrationParams.newBuilder()
                            .setFortinetIntegrationDetails(
                                FortinetIntegrationDetails.newBuilder()
                                    .setFortinetAuthCredentials(getFortinetAuthCredentials())
                                    .setFortinetRuleDetails(
                                        FortinetRuleDetails.newBuilder()
                                            .setFortinetTemplate(getFortinetTemplateRuleDetails())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest2, REQUEST_CONTEXT, existingWafIntegrations));
  }

  private FortinetAuthCredentials getFortinetAuthCredentials() {
    return FortinetAuthCredentials.newBuilder()
        .setEncryptionKeyId(ENCRYPTION_KEY_ID)
        .setEncryptedApiKey(ENCRYPTED_API_KEY)
        .build();
  }

  private FortinetApplication getFortinetApplicationRuleDetails() {
    return FortinetApplication.newBuilder().setApplicationId(APPLICATION_ID).build();
  }

  private FortinetTemplate getFortinetTemplateRuleDetails() {
    return FortinetTemplate.newBuilder().setTemplateId(TEMPLATE_ID).build();
  }

  @Test
  void invalidUpdateAzureRequestTest() {

    // empty azure integration params list
    UpdateWafIntegrationRequest request1 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId(WAF_INTEGRATION_ID)
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setUpdatedAzureIntegrationParams(
                        AzureIntegrationUpdateParams.getDefaultInstance()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request1, REQUEST_CONTEXT, existingWafIntegrations));

    // fetching an existing azure waf integration to check against cases of updation to a name that
    // already exists
    // or a combination of tenantId, policyResourceGroup
    WafIntegration existingAzureWafIntegration = getExistingAzureWafIntegration();

    // updation to a name that already belongs to another azure waf
    UpdateWafIntegrationRequest request2 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId(WAF_INTEGRATION_ID)
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setUpdatedAzureIntegrationParams(
                        AzureIntegrationUpdateParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId(TENANT_ID)
                                    .setSubscriptionId(SUBSCRIPTION_ID)
                                    .setAzureEnvironment(AZURE_ENVIRONMENT)
                                    .setAuthCredentials(getAzureAuthCredentials())
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyName(WAF_POLICY)
                                            .setWafPolicyResourceGroupName(
                                                WAF_POLICY_RESOURCE_GROUP)
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request2, REQUEST_CONTEXT, List.of(existingAzureWafIntegration)));

    // updation of a waf that contains the same combination of tenantId, wafPolicyResourceGroup and
    // wafPolicyType
    UpdateWafIntegrationRequest request3 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId(WAF_INTEGRATION_ID)
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setUpdatedAzureIntegrationParams(
                        AzureIntegrationUpdateParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId(TENANT_ID)
                                    .setSubscriptionId(SUBSCRIPTION_ID)
                                    .setAzureEnvironment(AZURE_ENVIRONMENT)
                                    .setAuthCredentials(getAzureAuthCredentials())
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyName(WAF_POLICY)
                                            .setWafPolicyResourceGroupName(
                                                WAF_POLICY_RESOURCE_GROUP)
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request3, REQUEST_CONTEXT, List.of(existingAzureWafIntegration)));

    // valid request - 1 of the above 3 parameters is different
    UpdateWafIntegrationRequest validRequest1 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId(WAF_INTEGRATION_ID)
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setUpdatedAzureIntegrationParams(
                        AzureIntegrationUpdateParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId(TENANT_ID)
                                    .setSubscriptionId(SUBSCRIPTION_ID)
                                    .setAzureEnvironment(AZURE_WAF_NAME)
                                    .setAuthCredentials(getAzureAuthCredentials())
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyName(WAF_POLICY)
                                            .setWafPolicyResourceGroupName(
                                                WAF_POLICY_RESOURCE_GROUP + "1")
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest1, REQUEST_CONTEXT, List.of(existingAzureWafIntegration)));

    // valid request
    UpdateWafIntegrationRequest validRequest2 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId(WAF_INTEGRATION_ID)
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName(AZURE_WAF_NAME)
                    .setUpdatedAzureIntegrationParams(
                        AzureIntegrationUpdateParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId(TENANT_ID)
                                    .setSubscriptionId(SUBSCRIPTION_ID)
                                    .setAzureEnvironment(AZURE_ENVIRONMENT)
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyName(WAF_POLICY)
                                            .setWafPolicyResourceGroupName(
                                                WAF_POLICY_RESOURCE_GROUP)
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId(CLIENT_ID)
                                            .setAccessKeyId(ACCESS_KEY)))))
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest2, REQUEST_CONTEXT, existingWafIntegrations));
  }

  @Test
  void invalidUpdateGcpRequestTest() {
    // empty gcp integration params list
    UpdateWafIntegrationRequest request1 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedGcpIntegrationParams(
                        GcpIntegrationUpdateParams.getDefaultInstance()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request1, REQUEST_CONTEXT, existingWafIntegrations));

    // valid request
    UpdateWafIntegrationRequest validRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedGcpIntegrationParams(
                        GcpIntegrationUpdateParams.newBuilder()
                            .setGcpIntegrationDetails(
                                GcpIntegrationDetails.newBuilder()
                                    .setProjectId("project-id")
                                    .setSecurityPolicyName("policy-name")
                                    .setDenyActionResponseCode(
                                        GcpIntegrationDetails.DenyActionResponseCode
                                            .DENY_ACTION_RESPONSE_CODE_403)
                                    .setAuthCredentials(
                                        GcpAuthCredentials.newBuilder()
                                            .setEncryptedServiceAccountKey(
                                                GcpAuthCredentials.EncryptedText.newBuilder()
                                                    .setKeyId("key-id")
                                                    .setValue("secret")
                                                    .build()))
                                    .setGlobalSecurityPolicyScope(
                                        GlobalSecurityPolicyScope.getDefaultInstance()))))
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest, REQUEST_CONTEXT, existingWafIntegrations));

    // request containing an existing security policy name should throw
    WafIntegration existingGcpWafIntegration = getExistingGcpWafIntegration();

    UpdateWafIntegrationRequest invalidRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedGcpIntegrationParams(
                        GcpIntegrationUpdateParams.newBuilder()
                            .setGcpIntegrationDetails(
                                GcpIntegrationDetails.newBuilder()
                                    .setProjectId(EXISTING_POLICY_ID)
                                    .setSecurityPolicyName(EXISTING_SECURITY_POLICY_NAME)
                                    .setDenyActionResponseCode(
                                        GcpIntegrationDetails.DenyActionResponseCode
                                            .DENY_ACTION_RESPONSE_CODE_403)
                                    .setAuthCredentials(
                                        GcpAuthCredentials.newBuilder()
                                            .setEncryptedServiceAccountKey(
                                                GcpAuthCredentials.EncryptedText.newBuilder()
                                                    .setKeyId("key-id")
                                                    .setValue("secret")
                                                    .build()))
                                    .setGlobalSecurityPolicyScope(
                                        GlobalSecurityPolicyScope.getDefaultInstance()))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest, REQUEST_CONTEXT, List.of(existingGcpWafIntegration)));
  }

  @Test
  void invalidUpdateF5RequestTest() {
    // empty f5 integration params list
    UpdateWafIntegrationRequest request1 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedF5IntegrationParams(F5IntegrationUpdateParams.getDefaultInstance()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request1, REQUEST_CONTEXT, existingWafIntegrations));

    // valid request
    UpdateWafIntegrationRequest validRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedF5IntegrationParams(
                        F5IntegrationUpdateParams.newBuilder()
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
                                            .setEncryptionKeyId("key-id")))
                            .build()))
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest, REQUEST_CONTEXT, existingWafIntegrations));

    // valid request without Auth credentials
    UpdateWafIntegrationRequest validRequest2 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedF5IntegrationParams(
                        F5IntegrationUpdateParams.newBuilder()
                            .setF5IntegrationDetails(
                                F5IntegrationDetails.newBuilder()
                                    .setUrl("https://localhost:9000")
                                    .setF5PolicyDetails(
                                        F5PolicyDetails.newBuilder()
                                            .setPolicyId("policy1")
                                            .setPolicyName("policyname")
                                            .build()))
                            .build()))
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest2, REQUEST_CONTEXT, existingWafIntegrations));

    // request updating policy name to an already existing one should throw
    WafIntegration existingF5WafIntegration = getExistingF5WafIntegration();
    UpdateWafIntegrationRequest invalidRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedF5IntegrationParams(
                        F5IntegrationUpdateParams.newBuilder()
                            .setF5IntegrationDetails(
                                F5IntegrationDetails.newBuilder()
                                    .setUrl(EXSITING_F5_URL)
                                    .setF5PolicyDetails(
                                        F5PolicyDetails.newBuilder()
                                            .setPolicyId("policy1")
                                            .setPolicyName(EXISTING_SECURITY_POLICY_NAME)
                                            .build())
                                    .setF5AuthCredentials(
                                        F5AuthCredentials.newBuilder()
                                            .setEncryptedUserName("user-name")
                                            .setEncryptedPassword("password")
                                            .setEncryptionKeyId("key-id")))
                            .build()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest, REQUEST_CONTEXT, List.of(existingF5WafIntegration)));
  }

  @Test
  void invalidUpdateFortinetRequestTest() {

    // empty integration params list
    UpdateWafIntegrationRequest invalidRequest1 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedFortinetIntegrationParams(
                        FortinetIntegrationUpdateParams.getDefaultInstance())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest1, REQUEST_CONTEXT, existingWafIntegrations));

    // empty integration details
    UpdateWafIntegrationRequest invalidRequest2 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedFortinetIntegrationParams(
                        FortinetIntegrationUpdateParams.newBuilder()
                            .clearFortinetIntegrationDetails()
                            .build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest2, REQUEST_CONTEXT, existingWafIntegrations));

    // empty rule details
    UpdateWafIntegrationRequest invalidRequest4 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedFortinetIntegrationParams(
                        FortinetIntegrationUpdateParams.newBuilder()
                            .setFortinetIntegrationDetails(
                                FortinetIntegrationDetails.newBuilder()
                                    .setFortinetAuthCredentials(getFortinetAuthCredentials())
                                    .clearFortinetRuleDetails()
                                    .build())
                            .build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest4, REQUEST_CONTEXT, existingWafIntegrations));

    // updation to an already existing name
    WafIntegration existingFortinetWafIntegration = getExistingFortinetWafIntegration();
    UpdateWafIntegrationRequest invalidRequest5 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedFortinetIntegrationParams(
                        FortinetIntegrationUpdateParams.newBuilder()
                            .setFortinetIntegrationDetails(
                                FortinetIntegrationDetails.newBuilder()
                                    .setFortinetAuthCredentials(getFortinetAuthCredentials())
                                    .setFortinetRuleDetails(
                                        FortinetRuleDetails.newBuilder()
                                            .setFortinetTemplate(getFortinetTemplateRuleDetails())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                invalidRequest5, REQUEST_CONTEXT, List.of(existingFortinetWafIntegration)));

    // valid updation request
    UpdateWafIntegrationRequest validRequest1 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id1")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name1")
                    .setUpdatedFortinetIntegrationParams(
                        FortinetIntegrationUpdateParams.newBuilder()
                            .setFortinetIntegrationDetails(
                                FortinetIntegrationDetails.newBuilder()
                                    .setFortinetAuthCredentials(getFortinetAuthCredentials())
                                    .setFortinetRuleDetails(
                                        FortinetRuleDetails.newBuilder()
                                            .setFortinetApplication(
                                                FortinetApplication.newBuilder()
                                                    .setApplicationId("application-id1")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest1, REQUEST_CONTEXT, List.of(existingFortinetWafIntegration)));

    // empty auth credentials
    UpdateWafIntegrationRequest validRequest2 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedFortinetIntegrationParams(
                        FortinetIntegrationUpdateParams.newBuilder()
                            .setFortinetIntegrationDetails(
                                FortinetIntegrationDetails.newBuilder()
                                    .clearFortinetAuthCredentials()
                                    .setFortinetRuleDetails(
                                        FortinetRuleDetails.newBuilder()
                                            .setFortinetApplication(
                                                getFortinetApplicationRuleDetails())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest2, REQUEST_CONTEXT, existingWafIntegrations));
  }

  private void testWithInvalidAwsResource(CreateWafIntegrationRequest request) {
    AwsResource.Builder awsResourceBuilder =
        AwsResource.newBuilder().setArn("arn").setRegion("region");

    AwsIntegrationParams.Builder awsIntegrationBuilder =
        AwsIntegrationParams.newBuilder()
            .setWebIdentityAuthCredentials(
                WebIdentityAuthenticationCredentials.newBuilder()
                    .setRoleArn("arn:aws:398429084503/role"));

    // no arn
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request.toBuilder()
                  .setWafIntegrationDetails(
                      request.getWafIntegrationDetails().toBuilder()
                          .setAwsIntegrationParams(
                              awsIntegrationBuilder.addResources(awsResourceBuilder.clearArn())))
                  .build(),
              REQUEST_CONTEXT,
              existingWafIntegrations);
        });

    // no region
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request.toBuilder()
                  .setWafIntegrationDetails(
                      request.getWafIntegrationDetails().toBuilder()
                          .setAwsIntegrationParams(
                              awsIntegrationBuilder.addResources(awsResourceBuilder.clearRegion())))
                  .build(),
              REQUEST_CONTEXT,
              existingWafIntegrations);
        });
  }

  private void testWithInvalidAwsResource(UpdateWafIntegrationRequest request) {
    AwsResource.Builder awsResourceBuilder =
        AwsResource.newBuilder().setArn("arn").setRegion("region");

    AwsIntegrationUpdateParams.Builder awsIntegrationBuilder =
        AwsIntegrationUpdateParams.newBuilder()
            .setWebIdentityAuthCredentials(
                WebIdentityAuthenticationCredentials.newBuilder().setRoleArn("aws:arn:342342"));

    // no arn
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request.toBuilder()
                  .setUpdatedWafIntegrationDetails(
                      request.getUpdatedWafIntegrationDetails().toBuilder()
                          .setUpdatedAwsIntegrationParams(
                              awsIntegrationBuilder.addResources(awsResourceBuilder.clearArn())))
                  .build(),
              REQUEST_CONTEXT,
              existingWafIntegrations);
        });

    // no arn
    assertThrows(
        StatusRuntimeException.class,
        () -> {
          wafIntegrationConfigRequestValidator.validateOrThrow(
              request.toBuilder()
                  .setUpdatedWafIntegrationDetails(
                      request.getUpdatedWafIntegrationDetails().toBuilder()
                          .setUpdatedAwsIntegrationParams(
                              awsIntegrationBuilder.addResources(awsResourceBuilder.clearRegion())))
                  .build(),
              REQUEST_CONTEXT,
              existingWafIntegrations);
        });
  }

  private WafIntegration getExistingGcpWafIntegration() {
    return WafIntegration.newBuilder()
        .setId("existingId")
        .setWafIntegrationDetails(
            WafIntegrationDetails.newBuilder()
                .setGcpIntegrationParams(
                    GcpIntegrationParams.newBuilder()
                        .setGcpIntegrationDetails(
                            GcpIntegrationDetails.newBuilder()
                                .setProjectId(EXISTING_POLICY_ID)
                                .setSecurityPolicyName(EXISTING_SECURITY_POLICY_NAME))))
        .build();
  }

  private WafIntegration getExistingF5WafIntegration() {
    return WafIntegration.newBuilder()
        .setId("existingId")
        .setWafIntegrationDetails(
            WafIntegrationDetails.newBuilder()
                .setF5IntegrationParams(
                    F5IntegrationParams.newBuilder()
                        .setF5IntegrationDetails(
                            F5IntegrationDetails.newBuilder()
                                .setUrl(EXSITING_F5_URL)
                                .setF5PolicyDetails(
                                    F5PolicyDetails.newBuilder()
                                        .setPolicyName(EXISTING_SECURITY_POLICY_NAME)))))
        .build();
  }

  private WafIntegration getExistingAkamaiWafIntegration() {
    return WafIntegration.newBuilder()
        .setId("existingId")
        .setWafIntegrationDetails(
            WafIntegrationDetails.newBuilder()
                .setAkamaiIntegrationParams(
                    AkamaiIntegrationParams.newBuilder()
                        .setAkamaiIntegrationDetails(
                            AkamaiIntegrationDetails.newBuilder()
                                .setHost("host")
                                .setAkamaiPolicyDetails(
                                    AkamaiPolicyDetails.newBuilder()
                                        .setPolicyId(EXISTING_POLICY_ID)))))
        .build();
  }

  private WafIntegration getExistingAzureWafIntegration() {
    return WafIntegration.newBuilder()
        .setId("existingId")
        .setWafIntegrationDetails(
            WafIntegrationDetails.newBuilder()
                .setName("name")
                .setAzureIntegrationParams(getAzureIntegrationParams())
                .build())
        .build();
  }

  /*
  .setApiId("api-id")
                            .setApiKey(
                                EncryptedText.newBuilder()
                                    .setKeyId("secret-id1")
                                    .setValue("secret-value1")
                                    .build())
   */

  private WafIntegration getExistingImpervaWafIntegration() {
    return WafIntegration.newBuilder()
        .setId("existingId")
        .setWafIntegrationDetails(
            WafIntegrationDetails.newBuilder()
                .setName("name")
                .setImpervaIntegrationParams(getImpervaIntegrationParams())
                .build())
        .build();
  }

  private ImpervaIntegrationParams getImpervaIntegrationParams() {
    return ImpervaIntegrationParams.newBuilder()
        .setApiId("api-id")
        .setApiKey(getImpervaApiKey())
        .setAccountId("account-id")
        .setWebsiteNames(
            StringList.newBuilder().addValues("website1.com").addValues("website2.com").build())
        .build();
  }

  private EncryptedText getImpervaApiKey() {
    return EncryptedText.newBuilder().setKeyId("secret-id").setValue("secret-value").build();
  }

  private AzureIntegrationParams getAzureIntegrationParams() {
    return AzureIntegrationParams.newBuilder()
        .addAzureIntegrationDetails(getAzureIntegrationDetails())
        .build();
  }

  private AzureIntegrationDetails getAzureIntegrationDetails() {
    return AzureIntegrationDetails.newBuilder()
        .setAzureTenantId(TENANT_ID)
        .setSubscriptionId(SUBSCRIPTION_ID)
        .setAzureEnvironment(AZURE_ENVIRONMENT)
        .setAzureWafPolicyDetails(getAzureWafPolicyDetails())
        .setAuthCredentials(getAzureAuthCredentials())
        .build();
  }

  private AzureAuthCredentials getAzureAuthCredentials() {
    return AzureAuthCredentials.newBuilder()
        .setClientId(CLIENT_ID)
        .setEncryptedClientSecret(CLIENT_SECRET)
        .setAccessKeyId(ACCESS_KEY)
        .build();
  }

  private AzureWafPolicyDetails getAzureWafPolicyDetails() {
    return AzureWafPolicyDetails.newBuilder()
        .setWafPolicyName(WAF_POLICY)
        .setWafPolicyResourceGroupName(WAF_POLICY_RESOURCE_GROUP)
        .setAzureWafPolicyType(AzureWafPolicyType.AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY)
        .build();
  }

  private WafIntegration getExistingFortinetWafIntegration() {
    return WafIntegration.newBuilder()
        .setId("existingId")
        .setWafIntegrationDetails(
            WafIntegrationDetails.newBuilder()
                .setName("name")
                .setFortinetIntegrationParams(getFortinetIntegrationParams())
                .build())
        .build();
  }

  private FortinetIntegrationParams getFortinetIntegrationParams() {
    return FortinetIntegrationParams.newBuilder()
        .setFortinetIntegrationDetails(getFortinetIntegrationDetails())
        .build();
  }

  private FortinetIntegrationDetails getFortinetIntegrationDetails() {
    return FortinetIntegrationDetails.newBuilder()
        .setFortinetAuthCredentials(getFortinetAuthCredentials())
        .setFortinetRuleDetails(
            FortinetRuleDetails.newBuilder()
                .setFortinetApplication(getFortinetApplicationRuleDetails())
                .build())
        .build();
  }
}
