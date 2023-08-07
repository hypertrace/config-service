package ai.traceable.waf.provider.integration.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.waf.integration.service.api.v1.AuthCredentials;
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
import ai.traceable.waf.integration.service.api.v1.WebIdentityAuthenticationCredentials;
import io.grpc.Status;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class WafIntegrationConfigRequestValidator {

  public void validateOrThrow(CreateWafIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateWafIntegrationDetails(request.getWafIntegrationDetails());
  }

  public void validateOrThrow(GetWafIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, GetWafIntegrationRequest.ID_FIELD_NUMBER);
  }

  public void validateOrThrow(GetWafIntegrationsRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateWafIntegrationsFilter(request.getFilter());
  }

  public void validateOrThrow(UpdateWafIntegrationRequest request, RequestContext requestContext) {
    validateRequestContextOrThrow(requestContext);
    validateNonDefaultPresenceOrThrow(request, UpdateWafIntegrationRequest.ID_FIELD_NUMBER);
    validateUpdateWafIntegrationDetails(request.getUpdatedWafIntegrationDetails());
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
      UpdatedWafIntegrationDetails updateWafIntegrationDetails) {
    validateNonDefaultPresenceOrThrow(
        updateWafIntegrationDetails, UpdatedWafIntegrationDetails.NAME_FIELD_NUMBER);
    validateUpdateIntegrationParams(updateWafIntegrationDetails);
  }

  private void validateWafIntegrationDetails(WafIntegrationDetails wafIntegrationDetails) {
    validateNonDefaultPresenceOrThrow(
        wafIntegrationDetails, WafIntegrationDetails.NAME_FIELD_NUMBER);
    validateIntegrationParams(wafIntegrationDetails);
  }

  private void validateWafProviderType(WafProviderType type) {
    switch (type) {
      case WAF_PROVIDER_TYPE_CLOUDFLARE:
      case WAF_PROVIDER_TYPE_AWS:
      case WAF_PROVIDER_TYPE_IMPERVA:
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
      UpdatedWafIntegrationDetails updatedWafIntegrationDetails) {
    switch (updatedWafIntegrationDetails.getIntegrationParamsCase()) {
      case UPDATED_CLOUDFLARE_INTEGRATION_PARAMS:
        validateUpdatedCloudFlareIntegrationParams(
            updatedWafIntegrationDetails.getUpdatedCloudflareIntegrationParams());
        break;
      case UPDATED_AWS_INTEGRATION_PARAMS:
        validateUpdatedAwsIntegrationParams(
            updatedWafIntegrationDetails.getUpdatedAwsIntegrationParams());
        break;
      case UPDATED_IMPERVA_INTEGRATION_PARAMS:
        validateUpdatedImpervaIntegrationParam(
            updatedWafIntegrationDetails.getUpdatedImpervaIntegrationParams());
        break;
      case INTEGRATIONPARAMS_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Unexpected integration params case: " + printMessage(updatedWafIntegrationDetails))
            .asRuntimeException();
    }
  }

  private void validateIntegrationParams(WafIntegrationDetails wafIntegrationDetails) {
    switch (wafIntegrationDetails.getIntegrationParamsCase()) {
      case CLOUDFLARE_INTEGRATION_PARAMS:
        validateCloudFlareIntegrationParams(wafIntegrationDetails.getCloudflareIntegrationParams());
        break;
      case AWS_INTEGRATION_PARAMS:
        validateAwsIntegrationParams(wafIntegrationDetails.getAwsIntegrationParams());
        break;
      case IMPERVA_INTEGRATION_PARAMS:
        validateImpervaIntegrationParam(wafIntegrationDetails.getImpervaIntegrationParams());
        break;
      case INTEGRATIONPARAMS_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Unexpected integration params case: " + printMessage(wafIntegrationDetails))
            .asRuntimeException();
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
      AwsIntegrationUpdateParams awsIntegrationUpdateParams) {
    validateNonDefaultPresenceOrThrow(
        awsIntegrationUpdateParams, AwsIntegrationUpdateParams.RESOURCES_FIELD_NUMBER);
    awsIntegrationUpdateParams.getResourcesList().forEach(this::validateAwsResource);
  }

  private void validateImpervaIntegrationParam(ImpervaIntegrationParams impervaIntegrationParams) {
    validateNonDefaultPresenceOrThrow(
        impervaIntegrationParams, ImpervaIntegrationParams.API_ID_FIELD_NUMBER);
    validateImpervaApiKey(impervaIntegrationParams.getApiKey());
  }

  private void validateCloudFlareIntegrationParams(
      CloudflareIntegrationParams cloudflareIntegrationParams) {
    validateNonDefaultPresenceOrThrow(
        cloudflareIntegrationParams, CloudflareIntegrationParams.ZONE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        cloudflareIntegrationParams, CloudflareIntegrationParams.EMAIL_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        cloudflareIntegrationParams, CloudflareIntegrationParams.API_TOKEN_FIELD_NUMBER);
  }

  private void validateAwsIntegrationParams(AwsIntegrationParams awsIntegrationParams) {
    validateCredentials(awsIntegrationParams);
    validateNonDefaultPresenceOrThrow(
        awsIntegrationParams, AwsIntegrationParams.RESOURCES_FIELD_NUMBER);
    awsIntegrationParams.getResourcesList().forEach(this::validateAwsResource);
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
