package ai.traceable.waf.provider.integration.service;

import ai.traceable.waf.integration.service.api.v1.AkamaiIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.AkamaiIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AkamaiIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.AuthCredentials;
import ai.traceable.waf.integration.service.api.v1.AwsIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AwsIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.AzureAuthCredentials;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.BarracudaIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.BarracudaIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.BarracudaIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.CloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.EncryptedData;
import ai.traceable.waf.integration.service.api.v1.F5IntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.F5IntegrationParams;
import ai.traceable.waf.integration.service.api.v1.F5IntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.FortinetAuthCredentials;
import ai.traceable.waf.integration.service.api.v1.FortinetIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.FortinetIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.FortinetIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.GcpAuthCredentials;
import ai.traceable.waf.integration.service.api.v1.GcpIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.GcpIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.GcpIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.ImpervaIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.ImpervaIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.RuleType;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.WafIntegration;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails.Builder;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails.IntegrationParamsCase;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationTarget;
import ai.traceable.waf.integration.service.api.v1.WebIdentityAuthenticationCredentials;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class WafIntegrationBuilderUtils {

  private static WafIntegrationDetails.Builder updateGenericFields(
      WafIntegrationDetails.Builder builder, UpdateWafIntegrationRequest request) {
    WafIntegrationDetails.Builder updatedWafIntegrationDetails =
        builder
            .setName(request.getUpdatedWafIntegrationDetails().getName())
            .setDescription(request.getUpdatedWafIntegrationDetails().getDescription());
    updateScope(updatedWafIntegrationDetails, request);
    return updateTargets(updatedWafIntegrationDetails, request);
  }

  public static WafIntegration getUpdatedIntegration(
      UpdateWafIntegrationRequest request, WafIntegration existingWafIntegration) {
    Builder updatedWafIntegrationDetailsBuilder =
        existingWafIntegration.getWafIntegrationDetails().toBuilder();
    updatedWafIntegrationDetailsBuilder =
        updateGenericFields(updatedWafIntegrationDetailsBuilder, request);

    switch (request.getUpdatedWafIntegrationDetails().getIntegrationParamsCase()) {
      case UPDATED_CLOUDFLARE_INTEGRATION_PARAMS:
        updateCloudflareWafIntegration(request, updatedWafIntegrationDetailsBuilder);
        break;
      case UPDATED_AWS_INTEGRATION_PARAMS:
        updateAwsWafIntegration(request, updatedWafIntegrationDetailsBuilder);
        break;
      case UPDATED_IMPERVA_INTEGRATION_PARAMS:
        updateImpervaWafIntegration(request, updatedWafIntegrationDetailsBuilder);
        break;
      case UPDATED_AZURE_INTEGRATION_PARAMS:
        updateAzureWafIntegration(request, updatedWafIntegrationDetailsBuilder);
        break;
      case UPDATED_GCP_INTEGRATION_PARAMS:
        updateGcpWafIntegration(request, updatedWafIntegrationDetailsBuilder);
        break;
      case UPDATED_F5_INTEGRATION_PARAMS:
        updateF5WafIntegration(request, updatedWafIntegrationDetailsBuilder);
        break;
      case UPDATED_AKAMAI_INTEGRATION_PARAMS:
        updateAkamaiWafIntegration(request, updatedWafIntegrationDetailsBuilder);
        break;
      case UPDATED_FORTINET_INTEGRATION_PARAMS:
        updateFortinetWafIntegration(request, updatedWafIntegrationDetailsBuilder);
        break;
      case UPDATED_BARRACUDA_INTEGRATION_PARAMS:
        updateBarracudaWafIntegration(request, updatedWafIntegrationDetailsBuilder);
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Unrecognized/Invalid integration type: "
                    + request.getUpdatedWafIntegrationDetails().getIntegrationParamsCase())
            .asRuntimeException();
    }
    return WafIntegration.newBuilder()
        .setId(existingWafIntegration.getId())
        .setWafIntegrationDetails(updatedWafIntegrationDetailsBuilder)
        .build();
  }

  private static void updateF5WafIntegration(
      UpdateWafIntegrationRequest request, Builder detailsBuilder) {
    F5IntegrationUpdateParams updatedF5IntegrationParams =
        request.getUpdatedWafIntegrationDetails().getUpdatedF5IntegrationParams();
    detailsBuilder.setF5IntegrationParams(
        getUpdatedF5IntegrationParams(
            updatedF5IntegrationParams, detailsBuilder.getF5IntegrationParams()));
  }

  private static void updateAkamaiWafIntegration(
      UpdateWafIntegrationRequest request, Builder detailsBuilder) {
    AkamaiIntegrationUpdateParams updatedAkamaiIntegrationParams =
        request.getUpdatedWafIntegrationDetails().getUpdatedAkamaiIntegrationParams();
    detailsBuilder.setAkamaiIntegrationParams(
        getUpdatedAkamaiIntegrationParams(
            updatedAkamaiIntegrationParams, detailsBuilder.getAkamaiIntegrationParams()));
  }

  private static void updateBarracudaWafIntegration(
      UpdateWafIntegrationRequest request, Builder detailsBuilder) {
    BarracudaIntegrationUpdateParams updatedBarracudaIntegrationParams =
        request.getUpdatedWafIntegrationDetails().getUpdatedBarracudaIntegrationParams();
    detailsBuilder.setBarracudaIntegrationParams(
        getUpdatedBarracudaIntegrationParams(
            updatedBarracudaIntegrationParams, detailsBuilder.getBarracudaIntegrationParams()));
  }

  private static GcpIntegrationParams getUpdatedGcpIntegrationParams(
      GcpIntegrationUpdateParams updatedGcpIntegrationParams,
      GcpIntegrationParams gcpIntegrationParams) {
    GcpIntegrationDetails updatedGcpIntegrationDetails =
        updatedGcpIntegrationParams.getGcpIntegrationDetails();
    GcpIntegrationDetails existingGcpIntegrationDetails =
        gcpIntegrationParams.getGcpIntegrationDetails();
    GcpIntegrationDetails gcpIntegrationDetails =
        updatedGcpIntegrationDetails.toBuilder()
            .setAuthCredentials(
                updatedRequestHasAuthCredentials(updatedGcpIntegrationDetails)
                    ? updatedGcpIntegrationDetails.getAuthCredentials()
                    : existingGcpIntegrationDetails.getAuthCredentials())
            .build();
    return GcpIntegrationParams.newBuilder()
        .setGcpIntegrationDetails(gcpIntegrationDetails)
        .build();
  }

  private static F5IntegrationParams getUpdatedF5IntegrationParams(
      F5IntegrationUpdateParams updatedF5IntegrationParams,
      F5IntegrationParams existingF5IntegrationParams) {
    F5IntegrationDetails updatedF5IntegrationDetails =
        updatedF5IntegrationParams.getF5IntegrationDetails();
    F5IntegrationDetails existingF5IntegrationDetails =
        existingF5IntegrationParams.getF5IntegrationDetails();
    F5IntegrationDetails f5IntegrationDetails =
        updatedF5IntegrationDetails.toBuilder()
            .setF5AuthCredentials(
                updatedF5IntegrationDetails.hasF5AuthCredentials()
                    ? updatedF5IntegrationDetails.getF5AuthCredentials()
                    : existingF5IntegrationDetails.getF5AuthCredentials())
            .build();
    return F5IntegrationParams.newBuilder().setF5IntegrationDetails(f5IntegrationDetails).build();
  }

  private static AkamaiIntegrationParams getUpdatedAkamaiIntegrationParams(
      AkamaiIntegrationUpdateParams akamaiIntegrationUpdateParams,
      AkamaiIntegrationParams akamaiIntegrationParams) {
    AkamaiIntegrationDetails updatedAkamaiIntegrationDetails =
        akamaiIntegrationUpdateParams.getAkamaiIntegrationDetails();
    AkamaiIntegrationDetails existingAkamaiIntegrationDetails =
        akamaiIntegrationParams.getAkamaiIntegrationDetails();
    AkamaiIntegrationDetails akamaiIntegrationDetails =
        updatedAkamaiIntegrationDetails.toBuilder()
            .setAkamaiAuthCredentials(
                updatedAkamaiIntegrationDetails.hasAkamaiAuthCredentials()
                    ? updatedAkamaiIntegrationDetails.getAkamaiAuthCredentials()
                    : existingAkamaiIntegrationDetails.getAkamaiAuthCredentials())
            .build();
    return AkamaiIntegrationParams.newBuilder()
        .setAkamaiIntegrationDetails(akamaiIntegrationDetails)
        .build();
  }

  private static BarracudaIntegrationParams getUpdatedBarracudaIntegrationParams(
      BarracudaIntegrationUpdateParams updatedBarracudaIntegrationParams,
      BarracudaIntegrationParams existingBarracudaIntegrationParams) {
    BarracudaIntegrationDetails updatedBarracudaIntegrationDetails =
        updatedBarracudaIntegrationParams.getBarracudaIntegrationDetails();
    BarracudaIntegrationDetails existingBarracudaIntegrationDetails =
        existingBarracudaIntegrationParams.getBarracudaIntegrationDetails();
    BarracudaIntegrationDetails barracudaIntegrationDetails =
        updatedBarracudaIntegrationDetails.toBuilder()
            .setBarracudaAuthCredentials(
                updatedBarracudaIntegrationDetails.hasBarracudaAuthCredentials()
                    ? updatedBarracudaIntegrationDetails.getBarracudaAuthCredentials()
                    : existingBarracudaIntegrationDetails.getBarracudaAuthCredentials())
            .build();
    return BarracudaIntegrationParams.newBuilder()
        .setBarracudaIntegrationDetails(barracudaIntegrationDetails)
        .build();
  }

  private static FortinetIntegrationParams getUpdatedFortinetIntegrationParams(
      FortinetIntegrationUpdateParams updatedFortinetIntegrationParams,
      FortinetIntegrationParams fortinetIntegrationParams) {
    FortinetIntegrationDetails updatedFortinetIntegrationDetails =
        updatedFortinetIntegrationParams.getFortinetIntegrationDetails();
    FortinetIntegrationDetails existingFortinetIntegrationDetails =
        fortinetIntegrationParams.getFortinetIntegrationDetails();
    FortinetIntegrationDetails fortinetIntegrationDetails =
        updatedFortinetIntegrationDetails.toBuilder()
            .setFortinetAuthCredentials(
                updatedFortinetIntegrationDetails.hasFortinetAuthCredentials()
                    ? updatedFortinetIntegrationDetails.getFortinetAuthCredentials()
                    : existingFortinetIntegrationDetails.getFortinetAuthCredentials())
            .build();
    return FortinetIntegrationParams.newBuilder()
        .setFortinetIntegrationDetails(fortinetIntegrationDetails)
        .build();
  }

  private static boolean updatedRequestHasAuthCredentials(
      GcpIntegrationDetails gcpIntegrationDetails) {
    return gcpIntegrationDetails.hasAuthCredentials()
        && gcpIntegrationDetails.getAuthCredentials().hasEncryptedServiceAccountKey();
  }

  private static boolean updatedRequestHasAuthCredentials(
      F5IntegrationDetails f5IntegrationDetails) {
    return f5IntegrationDetails.hasF5AuthCredentials();
  }

  private static void updateCloudflareWafIntegration(
      UpdateWafIntegrationRequest request, WafIntegrationDetails.Builder builder) {
    String apiToken =
        builder.getCloudflareIntegrationParams().getEncryptedApiToken().getBase64EncryptedData();
    String keyId = builder.getCloudflareIntegrationParams().getEncryptedApiToken().getKeyId();
    if (request
        .getUpdatedWafIntegrationDetails()
        .getUpdatedCloudflareIntegrationParams()
        .hasEncryptedApiToken()) {
      apiToken =
          request
              .getUpdatedWafIntegrationDetails()
              .getUpdatedCloudflareIntegrationParams()
              .getEncryptedApiToken()
              .getBase64EncryptedData();
      keyId =
          request
              .getUpdatedWafIntegrationDetails()
              .getUpdatedCloudflareIntegrationParams()
              .getEncryptedApiToken()
              .getKeyId();
    }
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
            .setRulesetId(
                request
                    .getUpdatedWafIntegrationDetails()
                    .getUpdatedCloudflareIntegrationParams()
                    .getRulesetId())
            .setEncryptedApiToken(
                EncryptedData.newBuilder()
                    .setKeyId(keyId)
                    .setBase64EncryptedData(apiToken)
                    .build());

    builder.setCloudflareIntegrationParams(cloudFlareIntegrationParamsBuilder);
  }

  private static WafIntegrationDetails.Builder updateTargets(
      WafIntegrationDetails.Builder builder, UpdateWafIntegrationRequest request) {
    builder
        .clearIntegrationTargets()
        .addAllIntegrationTargets(
            request.getUpdatedWafIntegrationDetails().getIntegrationTargetsList());
    if (builder.getIntegrationParamsCase() == IntegrationParamsCase.AKAMAI_INTEGRATION_PARAMS
        || builder.getIntegrationParamsCase()
            == IntegrationParamsCase.FORTINET_INTEGRATION_PARAMS) {
      return populateAllTargetsIfEmptyList(builder.build()).toBuilder();
    }
    if (builder.getIntegrationParamsCase() != IntegrationParamsCase.AWS_INTEGRATION_PARAMS) {
      return populateTargetsIfEmptyList(builder.build()).toBuilder();
    }
    return builder;
  }

  private static void updateScope(
      WafIntegrationDetails.Builder builder, UpdateWafIntegrationRequest request) {
    builder.setWafIntegrationScope(
        request.getUpdatedWafIntegrationDetails().getWafIntegrationScope());
  }

  private static void updateAwsWafIntegration(
      UpdateWafIntegrationRequest request, Builder detailsBuilder) {
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
        awsIntegrationUpdateParams, awsIntegrationParamsBuilder, detailsBuilder);

    detailsBuilder.setAwsIntegrationParams(awsIntegrationParamsBuilder);
  }

  private static void updateImpervaWafIntegration(
      UpdateWafIntegrationRequest request, Builder detailsBuilder) {

    ImpervaIntegrationUpdateParams updatedImpervaIntegrationParams =
        request.getUpdatedWafIntegrationDetails().getUpdatedImpervaIntegrationParams();
    ImpervaIntegrationParams.Builder impervaIntegrationParamsBuilder =
        ImpervaIntegrationParams.newBuilder();

    impervaIntegrationParamsBuilder.setApiId(
        updatedImpervaIntegrationParams.hasApiId()
            ? updatedImpervaIntegrationParams.getApiId()
            : detailsBuilder.getImpervaIntegrationParams().getApiId());

    impervaIntegrationParamsBuilder.setApiKey(
        updatedImpervaIntegrationParams.hasApiKey()
            ? updatedImpervaIntegrationParams.getApiKey()
            : detailsBuilder.getImpervaIntegrationParams().getApiKey());

    if (updatedImpervaIntegrationParams.hasAccountId()) {
      impervaIntegrationParamsBuilder.setAccountId(updatedImpervaIntegrationParams.getAccountId());
    } else if (detailsBuilder.getImpervaIntegrationParams().hasAccountId()) {
      impervaIntegrationParamsBuilder.setAccountId(
          detailsBuilder.getImpervaIntegrationParams().getAccountId());
    }

    if (updatedImpervaIntegrationParams.hasWebsiteIds()) {
      impervaIntegrationParamsBuilder.setWebsiteIds(
          updatedImpervaIntegrationParams.getWebsiteIds());
    } else if (updatedImpervaIntegrationParams.hasWebsiteNames()) {
      impervaIntegrationParamsBuilder.setWebsiteNames(
          updatedImpervaIntegrationParams.getWebsiteNames());
    } else {
      if (detailsBuilder.getImpervaIntegrationParams().hasWebsiteIds()) {
        impervaIntegrationParamsBuilder.setWebsiteIds(
            detailsBuilder.getImpervaIntegrationParams().getWebsiteIds());
      } else if (detailsBuilder.getImpervaIntegrationParams().hasWebsiteNames()) {
        impervaIntegrationParamsBuilder.setWebsiteNames(
            detailsBuilder.getImpervaIntegrationParams().getWebsiteNames());
      }
    }

    detailsBuilder.setImpervaIntegrationParams(impervaIntegrationParamsBuilder.build());
  }

  private static void updateAzureWafIntegration(
      UpdateWafIntegrationRequest request, Builder detailsBuilder) {
    AzureIntegrationUpdateParams updatedAzureIntegrationParams =
        request.getUpdatedWafIntegrationDetails().getUpdatedAzureIntegrationParams();

    detailsBuilder.setAzureIntegrationParams(
        getUpdatedAzureIntegrationParams(
            updatedAzureIntegrationParams, detailsBuilder.getAzureIntegrationParams()));
  }

  private static void updateGcpWafIntegration(
      UpdateWafIntegrationRequest request, Builder detailsBuilder) {
    GcpIntegrationUpdateParams updatedGcpIntegrationParams =
        request.getUpdatedWafIntegrationDetails().getUpdatedGcpIntegrationParams();

    detailsBuilder.setGcpIntegrationParams(
        getUpdatedGcpIntegrationParams(
            updatedGcpIntegrationParams, detailsBuilder.getGcpIntegrationParams()));
  }

  private static void updateFortinetWafIntegration(
      UpdateWafIntegrationRequest request, Builder detailsBuilder) {
    FortinetIntegrationUpdateParams updatedFortinetIntegrationParams =
        request.getUpdatedWafIntegrationDetails().getUpdatedFortinetIntegrationParams();

    detailsBuilder.setFortinetIntegrationParams(
        getUpdatedFortinetIntegrationParams(
            updatedFortinetIntegrationParams, detailsBuilder.getFortinetIntegrationParams()));
  }

  private static AzureIntegrationParams getUpdatedAzureIntegrationParams(
      AzureIntegrationUpdateParams updatedAzureIntegrationParams,
      AzureIntegrationParams existingAzureIntegrationParams) {
    Map<String, AzureIntegrationDetails> existingAzureIntegrationDetailsMap =
        existingAzureIntegrationParams.getAzureIntegrationDetailsList().stream()
            .collect(
                Collectors.toUnmodifiableMap(AzureIntegrationDetails::getId, Function.identity()));

    List<AzureIntegrationDetails> updatedAzureIntegrationDetails =
        updatedAzureIntegrationParams.getAzureIntegrationDetailsList().stream()
            .map(
                azureIntegrationDetails -> {
                  // new azure integration detail
                  if (azureIntegrationDetails.getId().isEmpty()
                      && !azureIntegrationDetails
                          .getAuthCredentials()
                          .getEncryptedClientSecret()
                          .isEmpty()) {
                    return azureIntegrationDetails.toBuilder()
                        .setId(UUID.randomUUID().toString())
                        .build();
                  } else if (azureIntegrationDetails
                      .getAuthCredentials()
                      .getEncryptedClientSecret()
                      .isEmpty()) {
                    String existingEncryptedClientSecret =
                        Optional.ofNullable(
                                existingAzureIntegrationDetailsMap.get(
                                    azureIntegrationDetails.getId()))
                            .orElseThrow()
                            .getAuthCredentials()
                            .getEncryptedClientSecret();
                    AzureAuthCredentials updatedAzureAuthCredentials =
                        azureIntegrationDetails.getAuthCredentials().toBuilder()
                            .setEncryptedClientSecret(existingEncryptedClientSecret)
                            .build();
                    return azureIntegrationDetails.toBuilder()
                        .setAuthCredentials(updatedAzureAuthCredentials)
                        .build();
                  }
                  return azureIntegrationDetails;
                })
            .collect(Collectors.toUnmodifiableList());

    return AzureIntegrationParams.newBuilder()
        .addAllAzureIntegrationDetails(updatedAzureIntegrationDetails)
        .build();
  }

  private static void updateConnectionCredentials(
      AwsIntegrationUpdateParams awsIntegrationUpdateParams,
      AwsIntegrationParams.Builder builder,
      Builder existingWafIntegrationBuilder) {
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
                        ? existingWafIntegrationBuilder
                            .getAwsIntegrationParams()
                            .getAuthCredentials()
                            .getEncryptedSecretAccessKey()
                        : awsIntegrationUpdateParams
                            .getAuthCredentials()
                            .getEncryptedSecretAccessKey())
                .setEncryptionKeyId(
                    !awsIntegrationUpdateParams.getAuthCredentials().getEncryptionKeyId().isEmpty()
                        ? awsIntegrationUpdateParams.getAuthCredentials().getEncryptionKeyId()
                        : existingWafIntegrationBuilder
                            .getAwsIntegrationParams()
                            .getAuthCredentials()
                            .getEncryptionKeyId())
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
                        : getAWSEncryptedSecretAccessKey(existingWafIntegrationBuilder))
                .build());
        return;
      default:
        throw Status.INVALID_ARGUMENT.asRuntimeException();
    }
  }

  private static String getAWSEncryptedSecretAccessKey(Builder existingWafIntegrationBuilder) {
    AwsIntegrationParams awsIntegrationParams =
        existingWafIntegrationBuilder.getAwsIntegrationParams();
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
                    .setAwsIntegrationParams(convertedAwsIntegrationParamsBuilder.build())
                    .build())
            .build();
      case AKAMAI_INTEGRATION_PARAMS:
      case FORTINET_INTEGRATION_PARAMS:
        return wafIntegration.toBuilder()
            .setWafIntegrationDetails(
                populateAllTargetsIfEmptyList(wafIntegration.getWafIntegrationDetails()))
            .build();
      default:
        return wafIntegration.toBuilder()
            .setWafIntegrationDetails(
                populateTargetsIfEmptyList(wafIntegration.getWafIntegrationDetails()))
            .build();
    }
  }

  public static WafIntegration stripSecrets(WafIntegration wafIntegration) {
    switch (wafIntegration.getWafIntegrationDetails().getIntegrationParamsCase()) {
      case AWS_INTEGRATION_PARAMS:
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
      case AZURE_INTEGRATION_PARAMS:
        List<AzureIntegrationDetails> azureIntegrationDetailsList =
            wafIntegration
                .getWafIntegrationDetails()
                .getAzureIntegrationParams()
                .getAzureIntegrationDetailsList()
                .stream()
                .map(
                    azureIntegrationDetails -> {
                      AzureAuthCredentials azureAuthCredentials =
                          azureIntegrationDetails.getAuthCredentials();
                      return azureIntegrationDetails.toBuilder()
                          .setAuthCredentials(
                              azureAuthCredentials.toBuilder().clearEncryptedClientSecret())
                          .build();
                    })
                .collect(Collectors.toUnmodifiableList());
        return WafIntegration.newBuilder()
            .setId(wafIntegration.getId())
            .setWafIntegrationDetails(
                wafIntegration.getWafIntegrationDetails().toBuilder()
                    .setAzureIntegrationParams(
                        AzureIntegrationParams.newBuilder()
                            .addAllAzureIntegrationDetails(azureIntegrationDetailsList)))
            .build();
      case GCP_INTEGRATION_PARAMS:
        return getGcpWafIntegrationWithSecretsStripped(wafIntegration);
      case F5_INTEGRATION_PARAMS:
        return getF5WafIntegrationWithSecretsStripped(wafIntegration);
      case AKAMAI_INTEGRATION_PARAMS:
        return getAkamaiWafIntegrationWithSecretsStripped(wafIntegration);
      case FORTINET_INTEGRATION_PARAMS:
        return getFortinetWafIntegrationWithSecretsStripped(wafIntegration);
      case BARRACUDA_INTEGRATION_PARAMS:
        return getBarracudaWafIntegrationWithSecretsStripped(wafIntegration);
      default:
        return wafIntegration;
    }
  }

  private static WafIntegration getF5WafIntegrationWithSecretsStripped(
      WafIntegration wafIntegration) {
    F5IntegrationParams f5IntegrationParams =
        wafIntegration.getWafIntegrationDetails().getF5IntegrationParams();
    F5IntegrationDetails.Builder f5IntegrationDetailsBuilder =
        f5IntegrationParams.getF5IntegrationDetails().toBuilder();
    f5IntegrationDetailsBuilder.clearF5AuthCredentials();
    return WafIntegration.newBuilder()
        .setId(wafIntegration.getId())
        .setWafIntegrationDetails(
            wafIntegration.getWafIntegrationDetails().toBuilder()
                .setF5IntegrationParams(
                    f5IntegrationParams.toBuilder()
                        .setF5IntegrationDetails(f5IntegrationDetailsBuilder.build())
                        .build())
                .build())
        .build();
  }

  private static WafIntegration getAkamaiWafIntegrationWithSecretsStripped(
      WafIntegration wafIntegration) {
    AkamaiIntegrationParams akamaiIntegrationParams =
        wafIntegration.getWafIntegrationDetails().getAkamaiIntegrationParams();
    AkamaiIntegrationDetails.Builder akamaiIntegrationDetailsBuilder =
        akamaiIntegrationParams.getAkamaiIntegrationDetails().toBuilder();
    akamaiIntegrationDetailsBuilder.clearAkamaiAuthCredentials();
    return WafIntegration.newBuilder()
        .setId(wafIntegration.getId())
        .setWafIntegrationDetails(
            wafIntegration.getWafIntegrationDetails().toBuilder()
                .setAkamaiIntegrationParams(
                    akamaiIntegrationParams.toBuilder()
                        .setAkamaiIntegrationDetails(akamaiIntegrationDetailsBuilder)
                        .build())
                .build())
        .build();
  }

  private static WafIntegration getBarracudaWafIntegrationWithSecretsStripped(
      WafIntegration wafIntegration) {
    BarracudaIntegrationParams barracudaIntegrationParams =
        wafIntegration.getWafIntegrationDetails().getBarracudaIntegrationParams();
    BarracudaIntegrationDetails.Builder barracudaIntegrationDetailsBuilder =
        barracudaIntegrationParams.getBarracudaIntegrationDetails().toBuilder();
    barracudaIntegrationDetailsBuilder.clearBarracudaAuthCredentials();
    return WafIntegration.newBuilder()
        .setId(wafIntegration.getId())
        .setWafIntegrationDetails(
            wafIntegration.getWafIntegrationDetails().toBuilder()
                .setBarracudaIntegrationParams(
                    barracudaIntegrationParams.toBuilder()
                        .setBarracudaIntegrationDetails(barracudaIntegrationDetailsBuilder)
                        .build())
                .build())
        .build();
  }

  private static WafIntegration getGcpWafIntegrationWithSecretsStripped(
      WafIntegration wafIntegration) {
    GcpIntegrationParams gcpIntegrationParams =
        wafIntegration.getWafIntegrationDetails().getGcpIntegrationParams();
    GcpIntegrationDetails.Builder gcpIntegrationDetailsBuilder =
        gcpIntegrationParams.getGcpIntegrationDetails().toBuilder();
    GcpAuthCredentials gcpAuthCredentials =
        gcpIntegrationParams.getGcpIntegrationDetails().getAuthCredentials();
    gcpIntegrationDetailsBuilder.setAuthCredentials(
        gcpAuthCredentials.toBuilder().clearEncryptedServiceAccountKey());
    return WafIntegration.newBuilder()
        .setId(wafIntegration.getId())
        .setWafIntegrationDetails(
            wafIntegration.getWafIntegrationDetails().toBuilder()
                .setGcpIntegrationParams(
                    gcpIntegrationParams.toBuilder()
                        .setGcpIntegrationDetails(gcpIntegrationDetailsBuilder.build())
                        .build())
                .build())
        .build();
  }

  private static WafIntegration getFortinetWafIntegrationWithSecretsStripped(
      WafIntegration wafIntegration) {
    FortinetIntegrationParams fortinetIntegrationParams =
        wafIntegration.getWafIntegrationDetails().getFortinetIntegrationParams();
    FortinetIntegrationDetails.Builder fortinetIntegrationDetailsBuilder =
        fortinetIntegrationParams.getFortinetIntegrationDetails().toBuilder();
    FortinetAuthCredentials fortinetAuthCredentials =
        fortinetIntegrationParams.getFortinetIntegrationDetails().getFortinetAuthCredentials();
    fortinetIntegrationDetailsBuilder.setFortinetAuthCredentials(
        fortinetAuthCredentials.toBuilder().clearEncryptedApiKey());
    return WafIntegration.newBuilder()
        .setId(wafIntegration.getId())
        .setWafIntegrationDetails(
            wafIntegration.getWafIntegrationDetails().toBuilder()
                .setFortinetIntegrationParams(
                    fortinetIntegrationParams.toBuilder()
                        .setFortinetIntegrationDetails(fortinetIntegrationDetailsBuilder.build())
                        .build())
                .build())
        .build();
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
      case AZURE_INTEGRATION_PARAMS:
        AzureIntegrationParams convertedAzureIntegrationParams =
            populateAzureIntegrationDetailsId(
                wafIntegration.getWafIntegrationDetails().getAzureIntegrationParams());
        return WafIntegration.newBuilder()
            .setId(wafIntegration.getId())
            .setWafIntegrationDetails(
                populateTargetsIfEmptyList(
                    wafIntegration.getWafIntegrationDetails().toBuilder()
                        .setAzureIntegrationParams(convertedAzureIntegrationParams)
                        .build()))
            .build();
      case AKAMAI_INTEGRATION_PARAMS:
      case IMPERVA_INTEGRATION_PARAMS:
      case FORTINET_INTEGRATION_PARAMS:
      case GCP_INTEGRATION_PARAMS:
        return wafIntegration.toBuilder()
            .setWafIntegrationDetails(
                populateAllTargetsIfEmptyList(wafIntegration.getWafIntegrationDetails()))
            .build();
      default:
        return wafIntegration.toBuilder()
            .setWafIntegrationDetails(
                populateTargetsIfEmptyList(wafIntegration.getWafIntegrationDetails()))
            .build();
    }
  }

  private static WafIntegrationDetails populateTargetsIfEmptyList(
      WafIntegrationDetails wafIntegrationDetails) {
    // if waf-integration targets are empty then populate it with
    // values [ip_range, threat_actor]
    if (wafIntegrationDetails.getIntegrationTargetsList().isEmpty()) {
      return wafIntegrationDetails.toBuilder()
          .addIntegrationTargets(
              WafIntegrationTarget.newBuilder().setRuleTarget(RuleType.RULE_TYPE_IP_RANGE).build())
          .addIntegrationTargets(
              WafIntegrationTarget.newBuilder()
                  .setRuleTarget(RuleType.RULE_TYPE_THREAT_ACTORS)
                  .build())
          .build();
    }
    return wafIntegrationDetails;
  }

  private static WafIntegrationDetails populateAllTargetsIfEmptyList(
      WafIntegrationDetails wafIntegrationDetails) {
    // if waf-integration targets are empty then populate it with
    // values [ip_range, threat_actor, custom_signature]
    if (wafIntegrationDetails.getIntegrationTargetsList().isEmpty()) {
      return wafIntegrationDetails.toBuilder()
          .addIntegrationTargets(
              WafIntegrationTarget.newBuilder().setRuleTarget(RuleType.RULE_TYPE_IP_RANGE).build())
          .addIntegrationTargets(
              WafIntegrationTarget.newBuilder().setRuleTarget(RuleType.RULE_TYPE_CUSTOM_SIGNATURE))
          .addIntegrationTargets(
              WafIntegrationTarget.newBuilder()
                  .setRuleTarget(RuleType.RULE_TYPE_THREAT_ACTORS)
                  .build())
          .build();
    }
    return wafIntegrationDetails;
  }

  private static AzureIntegrationParams populateAzureIntegrationDetailsId(
      AzureIntegrationParams azureIntegrationParams) {
    List<AzureIntegrationDetails> azureIntegrationDetailsList =
        azureIntegrationParams.getAzureIntegrationDetailsList().stream()
            .map(
                azureIntegrationDetails ->
                    azureIntegrationDetails.toBuilder().setId(UUID.randomUUID().toString()).build())
            .collect(Collectors.toUnmodifiableList());

    return AzureIntegrationParams.newBuilder()
        .addAllAzureIntegrationDetails(azureIntegrationDetailsList)
        .build();
  }
}
