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
  private final WafIntegrationConfigRequestValidator wafIntegrationConfigRequestValidator;
  private List<WafIntegration> existingWafIntegrations = List.of();

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
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest, REQUEST_CONTEXT, existingWafIntegrations));
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
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest, REQUEST_CONTEXT, existingWafIntegrations));
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
                    .setName("name")
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
                    .setName("name")
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
                    .setName("name")
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
                    .setName("name")
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
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
                                            .setAccessKeyId("key-id")))))
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
                    .setName("name")
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId("tenant-id")
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
                                            .setAccessKeyId("key-id")))))
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
                    .setName("name")
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId("tenant-id")
                                    .setSubscriptionId("subscription-id")
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
                                            .setAccessKeyId("key-id")))))
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
                    .setName("name")
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
                                            .setEncryptedClientSecret("secret")
                                            .setAccessKeyId("key-id")))))
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
                    .setName("name")
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
                                            .setAccessKeyId("key-id")))))
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
                    .setName("name")
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
                                            .setEncryptedClientSecret("secret")))))
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
                    .setName("name")
                    .setWafIntegrationScope(
                        WafIntegrationScope.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of())))
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAzureIntegrationDetails(
                                AzureIntegrationDetails.newBuilder()
                                    .setAzureTenantId("tenant-id")
                                    .setSubscriptionId("subscription-id")
                                    .setAzureEnvironment("azure-env")
                                    .setAzureWafPolicyDetails(
                                        AzureWafPolicyDetails.newBuilder()
                                            .setWafPolicyResourceGroupName("policy-rg-name")
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId("client-id")
                                            .setEncryptedClientSecret("secret")
                                            .setAccessKeyId("key-id")))))
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
                    .setName("name")
                    .setWafIntegrationScope(
                        WafIntegrationScope.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of())))
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
                                            .setAzureWafPolicyType(
                                                AzureWafPolicyType
                                                    .AZURE_WAF_POLICY_TYPE_APPLICATION_GATEWAY))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId("client-id")
                                            .setEncryptedClientSecret("secret")
                                            .setAccessKeyId("key-id")))))
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
                    .setName("name")
                    .setWafIntegrationScope(
                        WafIntegrationScope.newBuilder()
                            .setEnvironmentScope(
                                EnvironmentScope.newBuilder().addAllEnvironmentIds(List.of())))
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
                                            .setWafPolicyResourceGroupName("policy-rg-name"))
                                    .setAuthCredentials(
                                        AzureAuthCredentials.newBuilder()
                                            .setClientId("client-id")
                                            .setEncryptedClientSecret("secret")
                                            .setAccessKeyId("key-id")))))
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                request13, REQUEST_CONTEXT, existingWafIntegrations));

    // valid request
    CreateWafIntegrationRequest validRequest =
        CreateWafIntegrationRequest.newBuilder()
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setWafIntegrationScope(
                        WafIntegrationScope.newBuilder()
                            .setEnvironmentScope(EnvironmentScope.getDefaultInstance()))
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
                                            .setAccessKeyId("key-id")))))
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest, REQUEST_CONTEXT, existingWafIntegrations));
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
                    .setUpdatedAzureIntegrationParams(
                        AzureIntegrationUpdateParams.newBuilder()
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
                                            .setAccessKeyId("key-id")))))
            .build();
    assertDoesNotThrow(
        () ->
            wafIntegrationConfigRequestValidator.validateOrThrow(
                validRequest, REQUEST_CONTEXT, existingWafIntegrations));
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
}
