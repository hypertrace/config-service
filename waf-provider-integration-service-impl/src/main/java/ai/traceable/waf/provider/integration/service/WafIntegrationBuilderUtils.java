package ai.traceable.waf.provider.integration.service;

import ai.traceable.waf.integration.service.api.v1.AwsIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AwsIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.CloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.WafIntegration;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails;
import io.grpc.Status;

public class WafIntegrationBuilderUtils {
  public static WafIntegration getUpdatedIntegration(
      UpdateWafIntegrationRequest request, WafIntegration existingWafIntegration) {
    WafIntegration updatedWafIntegration;
    switch (request.getUpdatedWafIntegrationDetails().getIntegrationParamsCase()) {
      case UPDATED_CLOUDFLARE_INTEGRATION_PARAMS:
        updatedWafIntegration = getUpdatedCloudflareWafIntegration(request, existingWafIntegration);
        break;
      case UPDATED_AWS_INTEGRATION_PARAMS:
        updatedWafIntegration = getUpdatedAwsWafIntegration(request, existingWafIntegration);
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Unrecognized/Invalid integration type: "
                    + request.getUpdatedWafIntegrationDetails().getIntegrationParamsCase())
            .asRuntimeException();
    }

    return updatedWafIntegration;
  }

  private static WafIntegration getUpdatedCloudflareWafIntegration(
      UpdateWafIntegrationRequest request, WafIntegration existingWafIntegration) {
    String apiToken =
        request
                .getUpdatedWafIntegrationDetails()
                .getUpdatedCloudflareIntegrationParams()
                .hasApiToken()
            ? request
                .getUpdatedWafIntegrationDetails()
                .getUpdatedCloudflareIntegrationParams()
                .getApiToken()
            : existingWafIntegration
                .getWafIntegrationDetails()
                .getCloudflareIntegrationParams()
                .getApiToken();

    WafIntegrationDetails.Builder builder =
        WafIntegrationDetails.newBuilder()
            .setName(request.getUpdatedWafIntegrationDetails().getName())
            .setDescription(request.getUpdatedWafIntegrationDetails().getDescription());
    CloudflareIntegrationParams.Builder cloudFlareIntegrationParamsBuilder =
        CloudflareIntegrationParams.newBuilder()
            .setEmail(
                request
                    .getUpdatedWafIntegrationDetails()
                    .getUpdatedCloudflareIntegrationParams()
                    .getEmail())
            .setZone(
                request
                    .getUpdatedWafIntegrationDetails()
                    .getUpdatedCloudflareIntegrationParams()
                    .getZone())
            .setApiToken(apiToken);

    builder.setCloudflareIntegrationParams(cloudFlareIntegrationParamsBuilder);

    return existingWafIntegration.toBuilder().setWafIntegrationDetails(builder).build();
  }

  private static WafIntegration getUpdatedAwsWafIntegration(
      UpdateWafIntegrationRequest request, WafIntegration existingWafIntegration) {
    AwsIntegrationUpdateParams awsIntegrationUpdateParams =
        request.getUpdatedWafIntegrationDetails().getUpdatedAwsIntegrationParams();
    return WafIntegration.newBuilder()
        .setId(existingWafIntegration.getId())
        .setWafIntegrationDetails(
            WafIntegrationDetails.newBuilder()
                .setDescription(request.getUpdatedWafIntegrationDetails().getDescription())
                .setName(request.getUpdatedWafIntegrationDetails().getName())
                .setAwsIntegrationParams(
                    AwsIntegrationParams.newBuilder()
                        .setAccessKeyId(awsIntegrationUpdateParams.getAccessKeyId())
                        .setEncryptedSecretAccessKey(
                            awsIntegrationUpdateParams.hasEncryptedSecretAccessKey()
                                ? awsIntegrationUpdateParams.getEncryptedSecretAccessKey()
                                : existingWafIntegration
                                    .getWafIntegrationDetails()
                                    .getAwsIntegrationParams()
                                    .getEncryptedSecretAccessKey())
                        .addAllResources(awsIntegrationUpdateParams.getResourcesList())))
        .build();
  }
}
