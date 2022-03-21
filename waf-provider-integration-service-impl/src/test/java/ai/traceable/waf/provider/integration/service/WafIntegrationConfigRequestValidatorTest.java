package ai.traceable.waf.provider.integration.service;

import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.traceable.waf.integration.service.api.v1.CloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.DeleteWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter.WafProviderType;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsRequest;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationRequest;
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
  }

  @Test
  void invalidWafIntegrationDetailsTest() {
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
  void invalidUpdateRequestTest() {
    UpdateWafIntegrationRequest request =
        UpdateWafIntegrationRequest.newBuilder()
            .setId("id-1")
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
            .setWafIntegrationDetails(
                WafIntegrationDetails.newBuilder()
                    .setName("name")
                    .setDescription("des")
                    .setCloudflareIntegrationParams(
                        CloudflareIntegrationParams.newBuilder()
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
}
