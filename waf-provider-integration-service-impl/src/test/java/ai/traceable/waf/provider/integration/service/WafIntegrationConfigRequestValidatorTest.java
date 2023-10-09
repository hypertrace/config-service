package ai.traceable.waf.provider.integration.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.waf.integration.service.api.v1.AuthCredentials;
import ai.traceable.waf.integration.service.api.v1.AwsIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AwsIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.AwsResource;
import ai.traceable.waf.integration.service.api.v1.AzureAuthCredentials;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.AzureResourceGroupDetails;
import ai.traceable.waf.integration.service.api.v1.CloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.DeleteWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.EncryptedText;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter.WafProviderType;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsRequest;
import ai.traceable.waf.integration.service.api.v1.ImpervaIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.ImpervaIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.UpdatedCloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.UpdatedWafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WebIdentityAuthenticationCredentials;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;

class WafIntegrationConfigRequestValidatorTest {
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("test-tenant");
  private final WafIntegrationConfigRequestValidator wafIntegrationConfigRequestValidator;

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
          wafIntegrationConfigRequestValidator.validateOrThrow(request1, REQUEST_CONTEXT);
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
          wafIntegrationConfigRequestValidator.validateOrThrow(request, REQUEST_CONTEXT);
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
          wafIntegrationConfigRequestValidator.validateOrThrow(request1, REQUEST_CONTEXT);
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
          wafIntegrationConfigRequestValidator.validateOrThrow(request2, REQUEST_CONTEXT);
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
          wafIntegrationConfigRequestValidator.validateOrThrow(request3, REQUEST_CONTEXT);
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
          wafIntegrationConfigRequestValidator.validateOrThrow(updateRequest, REQUEST_CONTEXT);
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
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request1, REQUEST_CONTEXT));

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
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request2, REQUEST_CONTEXT));

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
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request3, REQUEST_CONTEXT));

    // valid request
    CreateWafIntegrationRequest validRequest =
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
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(validRequest, REQUEST_CONTEXT));
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
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request1, REQUEST_CONTEXT));

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
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request2, REQUEST_CONTEXT));

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
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request4, REQUEST_CONTEXT));

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
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request5, REQUEST_CONTEXT));

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
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request6, REQUEST_CONTEXT));

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
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request7, REQUEST_CONTEXT));

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
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request8, REQUEST_CONTEXT));

    UpdateWafIntegrationRequest validRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedImpervaIntegrationParams(
                        ImpervaIntegrationUpdateParams.newBuilder()
                            .setApiId("api-id")
                            .setApiKey(
                                EncryptedText.newBuilder()
                                    .setKeyId("secret-id")
                                    .setValue("secret-value")
                                    .build())
                            .build()))
            .build();
    // valid request
    assertDoesNotThrow(
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(validRequest, REQUEST_CONTEXT));
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
          wafIntegrationConfigRequestValidator.validateOrThrow(request1, REQUEST_CONTEXT);
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
          wafIntegrationConfigRequestValidator.validateOrThrow(request2, REQUEST_CONTEXT);
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
          wafIntegrationConfigRequestValidator.validateOrThrow(request3, REQUEST_CONTEXT);
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
          wafIntegrationConfigRequestValidator.validateOrThrow(request4, REQUEST_CONTEXT);
        });

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
          wafIntegrationConfigRequestValidator.validateOrThrow(validRequest, REQUEST_CONTEXT);
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
              REQUEST_CONTEXT);
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
          wafIntegrationConfigRequestValidator.validateOrThrow(request1, REQUEST_CONTEXT);
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
          wafIntegrationConfigRequestValidator.validateOrThrow(request2, REQUEST_CONTEXT);
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
          wafIntegrationConfigRequestValidator.validateOrThrow(request4, REQUEST_CONTEXT);
        });

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
          wafIntegrationConfigRequestValidator.validateOrThrow(validRequest, REQUEST_CONTEXT);
        });
  }

  @Test
  void invalidCreateAzureRequestTest() {
    // empty azure integration params list
    CreateWafIntegrationRequest request1 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAzureIntegrationParams(AzureIntegrationParams.getDefaultInstance()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request1, REQUEST_CONTEXT));

    // missing azure tenant-id
    CreateWafIntegrationRequest request2 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setSubscriptionId("subscription-id")
                                    .setAzureEnvironment("azure-env")
                                    .addAzureResourceGroupDetails(
                                        AzureResourceGroupDetails.newBuilder()
                                            .setName("name")
                                            .setRegion("region"))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId("client-id")
                                            .setEncryptedClientSecret("secret")
                                            .setAccessKeyId("key-id")))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request2, REQUEST_CONTEXT));

    // missing azure subscription-id
    CreateWafIntegrationRequest request3 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId("tenant-id")
                                    .setAzureEnvironment("azure-env")
                                    .addAzureResourceGroupDetails(
                                        AzureResourceGroupDetails.newBuilder()
                                            .setName("name")
                                            .setRegion("region"))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId("client-id")
                                            .setEncryptedClientSecret("secret")
                                            .setAccessKeyId("key-id")))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request3, REQUEST_CONTEXT));

    // missing azure env
    CreateWafIntegrationRequest request4 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId("tenant-id")
                                    .setSubscriptionId("subscription-id")
                                    .addAzureResourceGroupDetails(
                                        AzureResourceGroupDetails.newBuilder()
                                            .setName("name")
                                            .setRegion("region"))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId("client-id")
                                            .setEncryptedClientSecret("secret")
                                            .setAccessKeyId("key-id")))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request4, REQUEST_CONTEXT));

    // empty resource group list
    CreateWafIntegrationRequest request5 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId("tenant-id")
                                    .setSubscriptionId("subscription-id")
                                    .setAzureEnvironment("azure-env")
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId("client-id")
                                            .setEncryptedClientSecret("secret")
                                            .setAccessKeyId("key-id")))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request5, REQUEST_CONTEXT));

    // missing resource group name
    CreateWafIntegrationRequest request6 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId("tenant-id")
                                    .setSubscriptionId("subscription-id")
                                    .setAzureEnvironment("azure-env")
                                    .addAzureResourceGroupDetails(
                                        AzureResourceGroupDetails.newBuilder().setRegion("region"))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId("client-id")
                                            .setEncryptedClientSecret("secret")
                                            .setAccessKeyId("key-id")))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request6, REQUEST_CONTEXT));

    // missing resource group region
    CreateWafIntegrationRequest request7 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId("tenant-id")
                                    .setSubscriptionId("subscription-id")
                                    .setAzureEnvironment("azure-env")
                                    .addAzureResourceGroupDetails(
                                        AzureResourceGroupDetails.newBuilder().setName("name"))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId("client-id")
                                            .setEncryptedClientSecret("secret")
                                            .setAccessKeyId("key-id")))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request7, REQUEST_CONTEXT));

    // missing azure client-id
    CreateWafIntegrationRequest request8 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
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
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setEncryptedClientSecret("secret")
                                            .setAccessKeyId("key-id")))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request8, REQUEST_CONTEXT));

    // missing azure client secret
    CreateWafIntegrationRequest request9 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
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
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId("client-id")
                                            .setAccessKeyId("key-id")))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request9, REQUEST_CONTEXT));

    // missing azure access key id
    CreateWafIntegrationRequest request10 =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
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
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId("client-id")
                                            .setEncryptedClientSecret("secret")))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request10, REQUEST_CONTEXT));

    // valid request
    CreateWafIntegrationRequest validRequest =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
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
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId("client-id")
                                            .setEncryptedClientSecret("secret")
                                            .setAccessKeyId("key-id")))))
            .build();
    assertDoesNotThrow(
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(validRequest, REQUEST_CONTEXT));
  }

  @Test
  void invalidUpdateAzureRequestTest() {
    // empty azure integration params list
    UpdateWafIntegrationRequest request1 =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedAzureIntegrationParams(
                        AzureIntegrationUpdateParams.getDefaultInstance()))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(request1, REQUEST_CONTEXT));

    // valid request
    UpdateWafIntegrationRequest validRequest =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id")
            .setUpdatedWafIntegrationDetails(
                UpdatedWafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setUpdatedAzureIntegrationParams(
                        AzureIntegrationUpdateParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId("tenant-id")
                                    .setSubscriptionId("subscription-id")
                                    .setAzureEnvironment("azure-env")
                                    .addAzureResourceGroupDetails(
                                        AzureResourceGroupDetails.newBuilder()
                                            .setName("name")
                                            .setRegion("region"))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId("client-id")
                                            .setAccessKeyId("key-id")))))
            .build();
    assertDoesNotThrow(
        () -> wafIntegrationConfigRequestValidator.validateOrThrow(validRequest, REQUEST_CONTEXT));
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
              REQUEST_CONTEXT);
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
              REQUEST_CONTEXT);
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
              REQUEST_CONTEXT);
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
              REQUEST_CONTEXT);
        });
  }
}
