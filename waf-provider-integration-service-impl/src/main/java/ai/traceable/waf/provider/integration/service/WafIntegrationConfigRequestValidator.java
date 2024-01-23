package ai.traceable.waf.provider.integration.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.waf.integration.service.api.v1.AuthCredentials;
import ai.traceable.waf.integration.service.api.v1.AwsIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AwsIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.AwsResource;
import ai.traceable.waf.integration.service.api.v1.AzureAuthCredentials;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AzureIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.AzureWafPolicyDetails;
import ai.traceable.waf.integration.service.api.v1.CloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.DeleteWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.EncryptedData;
import ai.traceable.waf.integration.service.api.v1.EncryptedText;
import ai.traceable.waf.integration.service.api.v1.EnvironmentScope;
import ai.traceable.waf.integration.service.api.v1.GcpAuthCredentials;
import ai.traceable.waf.integration.service.api.v1.GcpIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.GcpIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.GcpIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsDetailsRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter.WafProviderType;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsRequest;
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
import io.grpc.Status;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class WafIntegrationConfigRequestValidator {

  public void validateOrThrow(
      CreateWafIntegrationRequest request,
      RequestContext requestContext,
      List<WafIntegration> existingWafIntegrations) {
    validateRequestContextOrThrow(requestContext);
    validateWafIntegrationDetails(request.getWafIntegrationDetails(), existingWafIntegrations);
  }

  public void validateOrThrow(GetWafIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, GetWafIntegrationRequest.ID_FIELD_NUMBER);
  }

  public void validateOrThrow(GetWafIntegrationsRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateWafIntegrationsFilter(request.getFilter());
  }

  public void validateOrThrow(
      GetWafIntegrationsDetailsRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateWafIntegrationsFilter(request.getFilter());
  }

  public void validateOrThrow(
      UpdateWafIntegrationRequest request,
      RequestContext requestContext,
      List<WafIntegration> existingWafIntegrations) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateWafIntegrationRequest.ID_FIELD_NUMBER);
    validateUpdateWafIntegrationDetails(
        request.getId(), request.getUpdatedWafIntegrationDetails(), existingWafIntegrations);
  }

  public void validateOrThrow(DeleteWafIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, DeleteWafIntegrationRequest.ID_FIELD_NUMBER);
  }

  private void validateWafIntegrationsFilter(GetWafIntegrationsFilter filter) {
    for (String id : filter.getIdsList()) {
      if (id.isEmpty()) {
        throw Status.INVALID_ARGUMENT.withDescription("id can not be empty").asRuntimeException();
      }
    }
    filter.getWafProviderTypesList().forEach(this::validateWafProviderType);
  }

  private void validateUpdateWafIntegrationDetails(
      String id,
      UpdatedWafIntegrationDetails updateWafIntegrationDetails,
      List<WafIntegration> existingWafIntegrations) {
    validateNonDefaultPresenceOrThrow(
        updateWafIntegrationDetails, UpdatedWafIntegrationDetails.NAME_FIELD_NUMBER);
    validateUpdateIntegrationParams(id, updateWafIntegrationDetails, existingWafIntegrations);
    this.validateWafIntegrationScope(updateWafIntegrationDetails.getWafIntegrationScope());
  }

  private void validateWafIntegrationDetails(
      WafIntegrationDetails wafIntegrationDetails, List<WafIntegration> existingWafIntegrations) {
    validateNonDefaultPresenceOrThrow(
        wafIntegrationDetails, WafIntegrationDetails.NAME_FIELD_NUMBER);
    validateIntegrationParams(wafIntegrationDetails, existingWafIntegrations);
    this.validateWafIntegrationScope(wafIntegrationDetails.getWafIntegrationScope());
  }

  private void validateWafIntegrationScope(WafIntegrationScope wafConfigScope) {
    switch (wafConfigScope.getScopeCase()) {
      case ENVIRONMENT_SCOPE:
        this.validateEnvironmentScope(wafConfigScope.getEnvironmentScope());
        return;
      default:
    }
  }

  private void validateEnvironmentScope(EnvironmentScope environmentScope) {
    if (environmentScope.getEnvironmentIdsList().stream().anyMatch(String::isEmpty)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Environment id should not be empty string")
          .asRuntimeException();
    }
  }

  private void validateWafProviderType(WafProviderType type) {
    switch (type) {
      case WAF_PROVIDER_TYPE_CLOUDFLARE:
      case WAF_PROVIDER_TYPE_AWS:
      case WAF_PROVIDER_TYPE_IMPERVA:
      case WAF_PROVIDER_TYPE_AZURE:
      case WAF_PROVIDER_TYPE_GCP:
        break;
      case WAF_PROVIDER_TYPE_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected waf provider type case: " + type)
            .asRuntimeException();
    }
  }

  private void validateUpdateIntegrationParams(
      String id,
      UpdatedWafIntegrationDetails updatedWafIntegrationDetails,
      List<WafIntegration> existingWafIntegrations) {
    switch (updatedWafIntegrationDetails.getIntegrationParamsCase()) {
      case UPDATED_CLOUDFLARE_INTEGRATION_PARAMS:
        validateUpdatedCloudFlareIntegrationParams(
            updatedWafIntegrationDetails.getUpdatedCloudflareIntegrationParams());
        break;
      case UPDATED_AWS_INTEGRATION_PARAMS:
        validateUpdatedAwsIntegrationParams(
            id,
            updatedWafIntegrationDetails.getUpdatedAwsIntegrationParams(),
            existingWafIntegrations);
        break;
      case UPDATED_IMPERVA_INTEGRATION_PARAMS:
        validateUpdatedImpervaIntegrationParam(
            updatedWafIntegrationDetails.getUpdatedImpervaIntegrationParams());
        break;
      case UPDATED_AZURE_INTEGRATION_PARAMS:
        validateUpdatedAzureIntegrationParams(
            updatedWafIntegrationDetails.getUpdatedAzureIntegrationParams());
        break;
      case UPDATED_GCP_INTEGRATION_PARAMS:
        validateUpdatedGcpIntegrationParams(
            updatedWafIntegrationDetails.getUpdatedGcpIntegrationParams());
        break;
      case INTEGRATIONPARAMS_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Unexpected integration params case: " + printMessage(updatedWafIntegrationDetails))
            .asRuntimeException();
    }
  }

  private void validateUpdatedGcpIntegrationParams(
      GcpIntegrationUpdateParams gcpIntegrationUpdateParams) {
    validateUpdatedGcpIntegrationDetails(gcpIntegrationUpdateParams.getGcpIntegrationDetails());
  }

  private void validateUpdatedGcpIntegrationDetails(GcpIntegrationDetails gcpIntegrationDetails) {
    validateNonDefaultPresenceOrThrow(
        gcpIntegrationDetails, GcpIntegrationDetails.PROJECT_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        gcpIntegrationDetails, GcpIntegrationDetails.SECURITY_POLICY_NAME_FIELD_NUMBER);
    validateUpdatedGcpAuthCredentials(gcpIntegrationDetails.getAuthCredentials());
    validateSecurityPolicyScope(gcpIntegrationDetails);
  }

  private void validateUpdatedGcpAuthCredentials(GcpAuthCredentials authCredentials) {
    if (authCredentials.hasEncryptedServiceAccountKey()) {
      validateGcpServiceAccountKey(authCredentials.getEncryptedServiceAccountKey());
    }
  }

  private void validateGcpServiceAccountKey(GcpAuthCredentials.EncryptedText serviceAccountKey) {
    validateNonDefaultPresenceOrThrow(serviceAccountKey, EncryptedText.KEY_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(serviceAccountKey, EncryptedText.VALUE_FIELD_NUMBER);
  }

  private void validateIntegrationParams(
      WafIntegrationDetails wafIntegrationDetails, List<WafIntegration> existingWafIntegrations) {
    switch (wafIntegrationDetails.getIntegrationParamsCase()) {
      case CLOUDFLARE_INTEGRATION_PARAMS:
        validateCloudFlareIntegrationParams(wafIntegrationDetails.getCloudflareIntegrationParams());
        break;
      case AWS_INTEGRATION_PARAMS:
        validateAwsIntegrationParams(
            wafIntegrationDetails.getAwsIntegrationParams(), existingWafIntegrations);
        break;
      case IMPERVA_INTEGRATION_PARAMS:
        validateImpervaIntegrationParam(wafIntegrationDetails.getImpervaIntegrationParams());
        break;
      case AZURE_INTEGRATION_PARAMS:
        validateAzureIntegrationParam(wafIntegrationDetails.getAzureIntegrationParams());
        break;
      case GCP_INTEGRATION_PARAMS:
        validateGcpIntegrationParams(wafIntegrationDetails.getGcpIntegrationParams());
        break;
      case INTEGRATIONPARAMS_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Unexpected integration params case: " + printMessage(wafIntegrationDetails))
            .asRuntimeException();
    }
  }

  private void validateGcpIntegrationParams(GcpIntegrationParams gcpIntegrationParams) {
    validateGcpIntegrationDetails(gcpIntegrationParams.getGcpIntegrationDetails());
  }

  private void validateGcpIntegrationDetails(GcpIntegrationDetails gcpIntegrationDetails) {
    validateNonDefaultPresenceOrThrow(
        gcpIntegrationDetails, GcpIntegrationDetails.PROJECT_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        gcpIntegrationDetails, GcpIntegrationDetails.SECURITY_POLICY_NAME_FIELD_NUMBER);
    validateGcpAuthCredentials(gcpIntegrationDetails.getAuthCredentials());
    validateSecurityPolicyScope(gcpIntegrationDetails);
  }

  private void validateSecurityPolicyScope(GcpIntegrationDetails gcpIntegrationDetails) {
    switch ((gcpIntegrationDetails.getSecurityPolicyScopeCase())) {
      case REGION_SECURITY_POLICY_SCOPE:
        validateNonDefaultPresenceOrThrow(
            gcpIntegrationDetails.getRegionSecurityPolicyScope(),
            RegionSecurityPolicyScope.REGION_FIELD_NUMBER);
        break;
      case GLOBAL_SECURITY_POLICY_SCOPE:
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Security policy scope missing")
            .asRuntimeException();
    }
  }

  private void validateGcpAuthCredentials(GcpAuthCredentials authCredentials) {
    if (authCredentials.hasEncryptedServiceAccountKey()) {
      validateGcpServiceAccountKey(authCredentials.getEncryptedServiceAccountKey());
    }
  }

  private void validateUpdatedCloudFlareIntegrationParams(
      UpdatedCloudflareIntegrationParams cloudflareIntegrationParams) {
    validateNonDefaultPresenceOrThrow(
        cloudflareIntegrationParams, CloudflareIntegrationParams.ZONE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        cloudflareIntegrationParams, CloudflareIntegrationParams.EMAIL_FIELD_NUMBER);
    if (cloudflareIntegrationParams.hasApiToken()
        && cloudflareIntegrationParams.getApiToken().isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Api token is empty! " + printMessage(cloudflareIntegrationParams))
          .asRuntimeException();
    }
    if (cloudflareIntegrationParams.hasEncryptedApiToken()) {
      this.validateEncryptedData(cloudflareIntegrationParams.getEncryptedApiToken());
    }
  }

  private void validateEncryptedData(EncryptedData encryptedData) {
    validateNonDefaultPresenceOrThrow(encryptedData, EncryptedData.KEY_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        encryptedData, EncryptedData.BASE64_ENCRYPTED_DATA_FIELD_NUMBER);
  }

  private void validateUpdatedAzureIntegrationParams(
      AzureIntegrationUpdateParams azureIntegrationUpdateParams) {
    validateNonDefaultPresenceOrThrow(
        azureIntegrationUpdateParams,
        AzureIntegrationUpdateParams.AZURE_INTEGRATION_DETAILS_FIELD_NUMBER);
    azureIntegrationUpdateParams
        .getAzureIntegrationDetailsList()
        .forEach(this::validateAzureIntegrationDetails);
    azureIntegrationUpdateParams.getAzureIntegrationDetailsList().stream()
        .map(AzureIntegrationDetails::getAuthCredentials)
        .forEach(this::validateUpdatedAzureAuthCredentials);
  }

  private void validateUpdatedImpervaIntegrationParam(
      ImpervaIntegrationUpdateParams impervaIntegrationParams) {
    if (impervaIntegrationParams.hasApiId()) {
      validateNonDefaultPresenceOrThrow(
          impervaIntegrationParams, ImpervaIntegrationUpdateParams.API_ID_FIELD_NUMBER);
    }
    if (impervaIntegrationParams.hasApiKey()) {
      validateImpervaApiKey(impervaIntegrationParams.getApiKey());
    }
  }

  private void validateImpervaApiKey(EncryptedText apiKey) {
    validateNonDefaultPresenceOrThrow(apiKey, EncryptedText.KEY_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(apiKey, EncryptedText.VALUE_FIELD_NUMBER);
  }

  private void validateUpdatedAwsIntegrationParams(
      String id,
      AwsIntegrationUpdateParams awsIntegrationUpdateParams,
      List<WafIntegration> existingWafIntegrations) {
    validateNonDefaultPresenceOrThrow(
        awsIntegrationUpdateParams, AwsIntegrationUpdateParams.RESOURCES_FIELD_NUMBER);
    awsIntegrationUpdateParams.getResourcesList().forEach(this::validateAwsResource);
    validateArnAlreadyExistInAwsWafIntegration(
        id, awsIntegrationUpdateParams.getResourcesList(), existingWafIntegrations);
  }

  private void validateImpervaIntegrationParam(ImpervaIntegrationParams impervaIntegrationParams) {
    validateNonDefaultPresenceOrThrow(
        impervaIntegrationParams, ImpervaIntegrationParams.API_ID_FIELD_NUMBER);
    validateImpervaApiKey(impervaIntegrationParams.getApiKey());
  }

  private void validateAzureIntegrationParam(AzureIntegrationParams azureIntegrationParams) {
    validateNonDefaultPresenceOrThrow(
        azureIntegrationParams, AzureIntegrationParams.AZURE_INTEGRATION_DETAILS_FIELD_NUMBER);
    azureIntegrationParams
        .getAzureIntegrationDetailsList()
        .forEach(this::validateAzureIntegrationDetails);
    azureIntegrationParams.getAzureIntegrationDetailsList().stream()
        .map(AzureIntegrationDetails::getAuthCredentials)
        .forEach(this::validateAzureAuthCredentials);
  }

  private void validateAzureIntegrationDetails(AzureIntegrationDetails azureIntegrationDetails) {
    validateNonDefaultPresenceOrThrow(
        azureIntegrationDetails, AzureIntegrationDetails.AZURE_TENANT_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        azureIntegrationDetails, AzureIntegrationDetails.SUBSCRIPTION_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        azureIntegrationDetails, AzureIntegrationDetails.AZURE_ENVIRONMENT_FIELD_NUMBER);
    validateAzureWafPolicyDetails(azureIntegrationDetails.getAzureWafPolicyDetails());
  }

  private void validateAzureWafPolicyDetails(AzureWafPolicyDetails azureWafPolicyDetails) {
    validateNonDefaultPresenceOrThrow(
        azureWafPolicyDetails, AzureWafPolicyDetails.WAF_POLICY_NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        azureWafPolicyDetails, AzureWafPolicyDetails.WAF_POLICY_RESOURCE_GROUP_NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        azureWafPolicyDetails, AzureWafPolicyDetails.AZURE_WAF_POLICY_TYPE_FIELD_NUMBER);
  }

  private void validateAzureAuthCredentials(AzureAuthCredentials azureAuthCredentials) {
    validateNonDefaultPresenceOrThrow(
        azureAuthCredentials, AzureAuthCredentials.CLIENT_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        azureAuthCredentials, AzureAuthCredentials.ENCRYPTED_CLIENT_SECRET_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        azureAuthCredentials, AzureAuthCredentials.ACCESS_KEY_ID_FIELD_NUMBER);
  }

  private void validateUpdatedAzureAuthCredentials(AzureAuthCredentials azureAuthCredentials) {
    validateNonDefaultPresenceOrThrow(
        azureAuthCredentials, AzureAuthCredentials.CLIENT_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        azureAuthCredentials, AzureAuthCredentials.ACCESS_KEY_ID_FIELD_NUMBER);
  }

  private void validateCloudFlareIntegrationParams(
      CloudflareIntegrationParams cloudflareIntegrationParams) {
    validateNonDefaultPresenceOrThrow(
        cloudflareIntegrationParams, CloudflareIntegrationParams.ZONE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        cloudflareIntegrationParams, CloudflareIntegrationParams.EMAIL_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        cloudflareIntegrationParams, CloudflareIntegrationParams.API_TOKEN_FIELD_NUMBER);
    if (cloudflareIntegrationParams.hasEncryptedApiToken()) {
      // TODO: remove deprecated field, and add unconditional validation on new field
      this.validateEncryptedData(cloudflareIntegrationParams.getEncryptedApiToken());
    }
  }

  private void validateAwsIntegrationParams(
      AwsIntegrationParams awsIntegrationParams, List<WafIntegration> existingWafIntegrations) {
    validateCredentials(awsIntegrationParams);
    validateNonDefaultPresenceOrThrow(
        awsIntegrationParams, AwsIntegrationParams.RESOURCES_FIELD_NUMBER);
    awsIntegrationParams.getResourcesList().forEach(this::validateAwsResource);
    validateArnAlreadyExistInAwsWafIntegration(
        awsIntegrationParams.getResourcesList(), existingWafIntegrations);
  }

  private static void validateArnAlreadyExistInAwsWafIntegration(
      List<AwsResource> resources, List<WafIntegration> existingWafIntegrations) {
    existingWafIntegrations.stream()
        .filter(
            wafIntegration -> wafIntegration.getWafIntegrationDetails().hasAwsIntegrationParams())
        .forEach(wafIntegration -> validateArn(resources, wafIntegration));
  }

  private void validateArnAlreadyExistInAwsWafIntegration(
      String id, List<AwsResource> resources, List<WafIntegration> existingWafIntegrations) {
    existingWafIntegrations.stream()
        .filter(
            wafIntegration -> wafIntegration.getWafIntegrationDetails().hasAwsIntegrationParams())
        .forEach(
            wafIntegration -> {
              if (!wafIntegration.getId().equals(id)) {
                validateArn(resources, wafIntegration);
              }
            });
  }

  private static void validateArn(List<AwsResource> resources, WafIntegration wafIntegration) {
    wafIntegration
        .getWafIntegrationDetails()
        .getAwsIntegrationParams()
        .getResourcesList()
        .forEach(
            awsResource -> {
              if (resources.stream()
                  .anyMatch(resource -> resource.getArn().equals(awsResource.getArn()))) {
                throw Status.INVALID_ARGUMENT
                    .withDescription(
                        String.format(
                            "Aws waf integration already existing with arn : %s",
                            awsResource.getArn()))
                    .asRuntimeException();
              }
            });
  }

  private void validateAwsResource(AwsResource awsResource) {
    validateNonDefaultPresenceOrThrow(awsResource, AwsResource.ARN_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(awsResource, AwsResource.REGION_FIELD_NUMBER);
  }

  private void validateCredentials(AwsIntegrationParams awsIntegrationParams) {

    switch (awsIntegrationParams.getConnectionCredentialsCase()) {
      case AUTH_CREDENTIALS:
        validateNonDefaultPresenceOrThrow(
            awsIntegrationParams.getAuthCredentials(), AuthCredentials.ACCESS_KEY_ID_FIELD_NUMBER);
        validateNonDefaultPresenceOrThrow(
            awsIntegrationParams.getAuthCredentials(),
            AuthCredentials.ENCRYPTED_SECRET_ACCESS_KEY_FIELD_NUMBER);
        break;
      case WEB_IDENTITY_AUTH_CREDENTIALS:
        validateNonDefaultPresenceOrThrow(
            awsIntegrationParams.getWebIdentityAuthCredentials(),
            WebIdentityAuthenticationCredentials.ROLE_ARN_FIELD_NUMBER);
        break;
      case CONNECTIONCREDENTIALS_NOT_SET:
      default:
        validateNonDefaultPresenceOrThrow(
            awsIntegrationParams, AwsIntegrationParams.ACCESS_KEY_ID_FIELD_NUMBER);
        validateNonDefaultPresenceOrThrow(
            awsIntegrationParams, AwsIntegrationParams.ENCRYPTED_SECRET_ACCESS_KEY_FIELD_NUMBER);
    }
  }
}
