package ai.traceable.waf.provider.integration.service;

import ai.traceable.waf.integration.service.api.v1.AuthCredentials;
import ai.traceable.waf.integration.service.api.v1.AwsIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AwsIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.CloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.ImpervaIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.ImpervaIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.WafIntegration;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WebIdentityAuthenticationCredentials;
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
      case UPDATED_IMPERVA_INTEGRATION_PARAMS:
        updatedWafIntegration = getUpdatedImpervaWafIntegration(request, existingWafIntegration);
        break;
      case UPDATED_AZURE_INTEGRATION_PARAMS:
        updatedWafIntegration = getUpdatedAzureWafIntegration(request, existingWafIntegration);
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
    AwsIntegrationParams.Builder awsIntegrationParamsBuilder =
        AwsIntegrationParams.newBuilder()
            .addAllResources(awsIntegrationUpdateParams.getResourcesList())
            .setIntegrationActionType(awsIntegrationUpdateParams.getIntegrationActionType())
            .setSyncExistingBlockingData(awsIntegrationUpdateParams.getSyncExistingBlockingData());
    if (awsIntegrationUpdateParams.hasRuleGroupCapacity()) {
      awsIntegrationParamsBuilder.setRuleGroupCapacity(
          awsIntegrationUpdateParams.getRuleGroupCapacity());
    }
    updateConnectionCredentials(
        awsIntegrationUpdateParams, awsIntegrationParamsBuilder, existingWafIntegration);
    return WafIntegration.newBuilder()
        .setId(existingWafIntegration.getId())
        .setWafIntegrationDetails(
            WafIntegrationDetails.newBuilder()
                .setDescription(request.getUpdatedWafIntegrationDetails().getDescription())
                .setName(request.getUpdatedWafIntegrationDetails().getName())
                .setAwsIntegrationParams(awsIntegrationParamsBuilder.build()))
        .build();
  }

  private static WafIntegration getUpdatedImpervaWafIntegration(
      UpdateWafIntegrationRequest request, WafIntegration existingWafIntegration) {
    ImpervaIntegrationUpdateParams updatedImpervaIntegrationParams =
        request.getUpdatedWafIntegrationDetails().getUpdatedImpervaIntegrationParams();
    return WafIntegration.newBuilder()
        .setId(existingWafIntegration.getId())
        .setWafIntegrationDetails(
            WafIntegrationDetails.newBuilder()
                .setDescription(request.getUpdatedWafIntegrationDetails().getDescription())
                .setName(request.getUpdatedWafIntegrationDetails().getName())
                .setImpervaIntegrationParams(
                    ImpervaIntegrationParams.newBuilder()
                        .setApiId(
                            updatedImpervaIntegrationParams.hasApiId()
                                ? updatedImpervaIntegrationParams.getApiId()
                                : existingWafIntegration
                                    .getWafIntegrationDetails()
                                    .getImpervaIntegrationParams()
                                    .getApiId())
                        .setApiKey(
                            updatedImpervaIntegrationParams.hasApiKey()
                                ? updatedImpervaIntegrationParams.getApiKey()
                                : existingWafIntegration
                                    .getWafIntegrationDetails()
                                    .getImpervaIntegrationParams()
                                    .getApiKey())))
        .build();
  }

  private static WafIntegration getUpdatedAzureWafIntegration(
      UpdateWafIntegrationRequest request, WafIntegration existingWafIntegration) {
    AzureIntegrationUpdateParams updatedAzureIntegrationParams =
        request.getUpdatedWafIntegrationDetails().getUpdatedAzureIntegrationParams();
    return WafIntegration.newBuilder()
        .setId(existingWafIntegration.getId())
        .setWafIntegrationDetails(
            WafIntegrationDetails.newBuilder()
                .setDescription(request.getUpdatedWafIntegrationDetails().getDescription())
                .setName(request.getUpdatedWafIntegrationDetails().getName())
                .setAzureIntegrationParams(
                    AzureIntegrationParams.newBuilder()
                        .addAllAzureIntegrationDetails(
                            updatedAzureIntegrationParams.getAzureIntegrationDetailsList())))
        .build();
  }

  private static void updateConnectionCredentials(
      AwsIntegrationUpdateParams awsIntegrationUpdateParams,
      AwsIntegrationParams.Builder builder,
      WafIntegration existingWafIntegration) {
    switch (awsIntegrationUpdateParams.getConnectionCredentialsCase()) {
      case AUTH_CREDENTIALS:
        builder.setAuthCredentials(
            AuthCredentials.newBuilder()
                .setAccessKeyId(awsIntegrationUpdateParams.getAuthCredentials().getAccessKeyId())
                .setEncryptedSecretAccessKey(
                    awsIntegrationUpdateParams
                            .getAuthCredentials()
                            .getEncryptedSecretAccessKey()
                            .isEmpty()
                        ? existingWafIntegration
                            .getWafIntegrationDetails()
                            .getAwsIntegrationParams()
                            .getAuthCredentials()
                            .getEncryptedSecretAccessKey()
                        : awsIntegrationUpdateParams
                            .getAuthCredentials()
                            .getEncryptedSecretAccessKey())
                .build());
        return;
      case WEB_IDENTITY_AUTH_CREDENTIALS:
        builder.setWebIdentityAuthCredentials(
            WebIdentityAuthenticationCredentials.newBuilder()
                .setRoleArn(awsIntegrationUpdateParams.getWebIdentityAuthCredentials().getRoleArn())
                .build());
        return;
      case CONNECTIONCREDENTIALS_NOT_SET:
        builder.setAuthCredentials(
            AuthCredentials.newBuilder()
                .setAccessKeyId(awsIntegrationUpdateParams.getAccessKeyId())
                .setEncryptedSecretAccessKey(
                    awsIntegrationUpdateParams.hasEncryptedSecretAccessKey()
                        ? awsIntegrationUpdateParams.getEncryptedSecretAccessKey()
                        : getAWSEncryptedSecretAccessKey(existingWafIntegration))
                .build());
        return;
      default:
        throw Status.INVALID_ARGUMENT.asRuntimeException();
    }
  }

  private static String getAWSEncryptedSecretAccessKey(WafIntegration existingWafIntegration) {
    AwsIntegrationParams awsIntegrationParams =
        existingWafIntegration.getWafIntegrationDetails().getAwsIntegrationParams();
    if (awsIntegrationParams.hasAuthCredentials()) {
      return awsIntegrationParams.getAuthCredentials().getEncryptedSecretAccessKey();
    }
    return awsIntegrationParams.getEncryptedSecretAccessKey();
  }

  public static WafIntegration getBackwardCompatibleWafIntegration(WafIntegration wafIntegration) {
    switch (wafIntegration.getWafIntegrationDetails().getIntegrationParamsCase()) {
      case AWS_INTEGRATION_PARAMS:
        AwsIntegrationParams awsIntegrationParams =
            wafIntegration.getWafIntegrationDetails().getAwsIntegrationParams();
        AwsIntegrationParams.Builder convertedAwsIntegrationParamsBuilder =
            awsIntegrationParams.toBuilder();
        if (!awsIntegrationParams.getAccessKeyId().isEmpty()) { // we are getting waf in old format
          convertedAwsIntegrationParamsBuilder.setAuthCredentials(
              getAuthCredentialsBuilder(awsIntegrationParams));
        } else if (awsIntegrationParams
            .getConnectionCredentialsCase()
            .equals(AwsIntegrationParams.ConnectionCredentialsCase.AUTH_CREDENTIALS)) {
          convertedAwsIntegrationParamsBuilder
              .setAccessKeyId(awsIntegrationParams.getAuthCredentials().getAccessKeyId())
              .setEncryptedSecretAccessKey(
                  awsIntegrationParams.getAuthCredentials().getEncryptedSecretAccessKey());
        }
        return WafIntegration.newBuilder()
            .setId(wafIntegration.getId())
            .setWafIntegrationDetails(
                wafIntegration.getWafIntegrationDetails().toBuilder()
                    .setAwsIntegrationParams(convertedAwsIntegrationParamsBuilder.build()))
            .build();
      default:
        return wafIntegration;
    }
  }

  public static WafIntegration stripSecrets(WafIntegration wafIntegration) {
    if (wafIntegration.getWafIntegrationDetails().hasAwsIntegrationParams()) {
      AwsIntegrationParams.Builder awsIntegrationParamsBuilder =
          wafIntegration.getWafIntegrationDetails().getAwsIntegrationParams().toBuilder();
      awsIntegrationParamsBuilder.clearEncryptedSecretAccessKey();
      AuthCredentials authCredentials = awsIntegrationParamsBuilder.getAuthCredentials();
      if (awsIntegrationParamsBuilder.hasAuthCredentials()) {
        awsIntegrationParamsBuilder.setAuthCredentials(
            authCredentials.toBuilder().clearEncryptedSecretAccessKey().build());
      }
      return WafIntegration.newBuilder()
          .setId(wafIntegration.getId())
          .setWafIntegrationDetails(
              wafIntegration.getWafIntegrationDetails().toBuilder()
                  .setAwsIntegrationParams(awsIntegrationParamsBuilder.build()))
          .build();
    }
    return wafIntegration;
  }

  private static AuthCredentials.Builder getAuthCredentialsBuilder(
      AwsIntegrationParams awsIntegrationParams) {
    AuthCredentials.Builder authCredentialsBuilder =
        AuthCredentials.newBuilder().setAccessKeyId(awsIntegrationParams.getAccessKeyId());
    authCredentialsBuilder.setEncryptedSecretAccessKey(
        awsIntegrationParams.getEncryptedSecretAccessKey());
    return authCredentialsBuilder;
  }

  public static WafIntegration getCreateWafIntegrationV2(
      WafIntegration
          wafIntegration) { // this is for supporting waf integration, should be removed once all
    // services are updated
    switch (wafIntegration.getWafIntegrationDetails().getIntegrationParamsCase()) {
      case AWS_INTEGRATION_PARAMS:
        AwsIntegrationParams awsIntegrationParams =
            wafIntegration.getWafIntegrationDetails().getAwsIntegrationParams();
        AwsIntegrationParams.Builder convertedAwsIntegrationParamsBuilder =
            awsIntegrationParams.toBuilder();
        if (!awsIntegrationParams.getAccessKeyId().isEmpty()) {
          convertedAwsIntegrationParamsBuilder.setAuthCredentials(
              getAuthCredentialsBuilder(awsIntegrationParams));
          convertedAwsIntegrationParamsBuilder.clearAccessKeyId();
          convertedAwsIntegrationParamsBuilder.clearEncryptedSecretAccessKey();
        }
        return WafIntegration.newBuilder()
            .setId(wafIntegration.getId())
            .setWafIntegrationDetails(
                wafIntegration.getWafIntegrationDetails().toBuilder()
                    .setAwsIntegrationParams(convertedAwsIntegrationParamsBuilder.build()))
            .build();
      default:
        return wafIntegration;
    }
  }
}
