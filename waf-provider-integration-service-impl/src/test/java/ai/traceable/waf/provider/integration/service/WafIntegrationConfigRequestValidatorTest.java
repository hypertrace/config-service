package ai.traceable.waf.provider.integration.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.waf.integration.service.api.v1.AwsIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AwsIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.AwsResource;
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
                            WafProviderType.WAF_PROVIDER_TYPE_IMPERVA)))
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
            .setAccessKeyId("access-key")
            .setEncryptedSecretAccessKey("secret")
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
                    .setAwsIntegrationParams(awsIntegrationParamsBuilder.clearAccessKeyId()))
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
                        awsIntegrationParamsBuilder.clearEncryptedSecretAccessKey()))
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
                            .setAccessKeyId("id")
                            .setEncryptedSecretAccessKey("key")
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
  void invalidUpdateAwsRequestTest() {
    AwsIntegrationUpdateParams.Builder awsIntegrationUpdateParams =
        AwsIntegrationUpdateParams.newBuilder()
            .setAccessKeyId("access-key")
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
                            .setAccessKeyId("id")
                            .setEncryptedSecretAccessKey("key")
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

  private void testWithInvalidAwsResource(CreateWafIntegrationRequest request) {
    AwsResource.Builder awsResourceBuilder =
        AwsResource.newBuilder().setArn("arn").setRegion("region");

    AwsIntegrationParams.Builder awsIntegrationBuilder =
        AwsIntegrationParams.newBuilder()
            .setAccessKeyId("access-key")
            .setEncryptedSecretAccessKey("secret");

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
            .setAccessKeyId("access-key")
            .setEncryptedSecretAccessKey("secret");

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
