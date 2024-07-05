package ai.traceable.waf.provider.integration.service;

import static ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails.IntegrationParamsCase.AKAMAI_INTEGRATION_PARAMS;
import static ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails.IntegrationParamsCase.AWS_INTEGRATION_PARAMS;
import static ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails.IntegrationParamsCase.AZURE_INTEGRATION_PARAMS;
import static ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails.IntegrationParamsCase.CLOUDFLARE_INTEGRATION_PARAMS;
import static ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails.IntegrationParamsCase.F5_INTEGRATION_PARAMS;
import static ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails.IntegrationParamsCase.GCP_INTEGRATION_PARAMS;
import static ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails.IntegrationParamsCase.IMPERVA_INTEGRATION_PARAMS;
import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.waf.integration.service.api.v1.AkamaiAuthCredentials;
import ai.traceable.waf.integration.service.api.v1.AkamaiIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.AkamaiIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.AkamaiIntegrationUpdateParams;
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
import ai.traceable.waf.integration.service.api.v1.CloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.DeleteWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.EncryptedData;
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
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsDetailsRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter.WafProviderType;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsRequest;
import ai.traceable.waf.integration.service.api.v1.ImpervaIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.ImpervaIntegrationUpdateParams;
import ai.traceable.waf.integration.service.api.v1.RegionSecurityPolicyScope;
import ai.traceable.waf.integration.service.api.v1.RuleType;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.UpdatedCloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.UpdatedWafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WafIntegration;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationScope;
import ai.traceable.waf.integration.service.api.v1.WebIdentityAuthenticationCredentials;
import io.grpc.Status;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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

  private void validateNonCustomSignatureIntegrationTargets(
      WafIntegrationDetails wafIntegrationDetails,
      WafIntegrationDetails.IntegrationParamsCase integrationParamsCase) {
    if (wafIntegrationDetails.getIntegrationTargetsList().stream()
        .anyMatch(
            wafIntegrationTarget ->
                wafIntegrationTarget.getRuleTarget() == RuleType.RULE_TYPE_CUSTOM_SIGNATURE)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "Custom signature rules not supported for waf type " + integrationParamsCase.name())
          .asRuntimeException();
    }
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
      case WAF_PROVIDER_TYPE_F5:
      case WAF_PROVIDER_TYPE_AKAMAI:
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
        existingWafIntegrations =
            getOtherWafIntegrationOfSameType(
                id, CLOUDFLARE_INTEGRATION_PARAMS, existingWafIntegrations);
        validateUniqueIntegrationName(
            updatedWafIntegrationDetails.getName(),
            existingWafIntegrations,
            CLOUDFLARE_INTEGRATION_PARAMS);
        validateUpdatedCloudFlareIntegrationParams(
            updatedWafIntegrationDetails.getUpdatedCloudflareIntegrationParams(),
            existingWafIntegrations);
        break;
      case UPDATED_AWS_INTEGRATION_PARAMS:
        existingWafIntegrations =
            getOtherWafIntegrationOfSameType(id, AWS_INTEGRATION_PARAMS, existingWafIntegrations);
        validateUniqueIntegrationName(
            updatedWafIntegrationDetails.getName(),
            existingWafIntegrations,
            AWS_INTEGRATION_PARAMS);
        validateUpdatedAwsIntegrationParams(
            id,
            updatedWafIntegrationDetails.getUpdatedAwsIntegrationParams(),
            existingWafIntegrations);
        break;
      case UPDATED_IMPERVA_INTEGRATION_PARAMS:
        existingWafIntegrations =
            getOtherWafIntegrationOfSameType(
                id, IMPERVA_INTEGRATION_PARAMS, existingWafIntegrations);
        validateUniqueIntegrationName(
            updatedWafIntegrationDetails.getName(),
            existingWafIntegrations,
            IMPERVA_INTEGRATION_PARAMS);
        validateUpdatedImpervaIntegrationParam(
            updatedWafIntegrationDetails.getUpdatedImpervaIntegrationParams());
        break;
      case UPDATED_AZURE_INTEGRATION_PARAMS:
        existingWafIntegrations =
            getOtherWafIntegrationOfSameType(id, AZURE_INTEGRATION_PARAMS, existingWafIntegrations);
        validateUniqueIntegrationName(
            updatedWafIntegrationDetails.getName(),
            existingWafIntegrations,
            AZURE_INTEGRATION_PARAMS);
        validateUpdatedAzureIntegrationParams(
            updatedWafIntegrationDetails.getUpdatedAzureIntegrationParams());
        break;
      case UPDATED_GCP_INTEGRATION_PARAMS:
        existingWafIntegrations =
            getOtherWafIntegrationOfSameType(id, GCP_INTEGRATION_PARAMS, existingWafIntegrations);
        validateUniqueIntegrationName(
            updatedWafIntegrationDetails.getName(),
            existingWafIntegrations,
            GCP_INTEGRATION_PARAMS);
        validateUpdatedGcpIntegrationParams(
            updatedWafIntegrationDetails.getUpdatedGcpIntegrationParams(), existingWafIntegrations);
        break;
      case UPDATED_F5_INTEGRATION_PARAMS:
        existingWafIntegrations =
            getOtherWafIntegrationOfSameType(id, F5_INTEGRATION_PARAMS, existingWafIntegrations);
        validateUniqueIntegrationName(
            updatedWafIntegrationDetails.getName(), existingWafIntegrations, F5_INTEGRATION_PARAMS);
        validateUpdatedF5IntegrationParams(
            updatedWafIntegrationDetails.getUpdatedF5IntegrationParams(), existingWafIntegrations);
        break;
      case UPDATED_AKAMAI_INTEGRATION_PARAMS:
        existingWafIntegrations =
            getOtherWafIntegrationOfSameType(
                id, AKAMAI_INTEGRATION_PARAMS, existingWafIntegrations);
        validateUniqueIntegrationName(
            updatedWafIntegrationDetails.getName(),
            existingWafIntegrations,
            AKAMAI_INTEGRATION_PARAMS);
        validateUpdatedAkamaiIntegrationParams(
            updatedWafIntegrationDetails.getUpdatedAkamaiIntegrationParams(),
            existingWafIntegrations);
        break;
      case INTEGRATIONPARAMS_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Unexpected integration params case: " + printMessage(updatedWafIntegrationDetails))
            .asRuntimeException();
    }
  }

  private List<WafIntegration> getOtherWafIntegrationOfSameType(
      @Nullable String id,
      WafIntegrationDetails.IntegrationParamsCase integrationParamsCase,
      List<WafIntegration> existingWafIntegrations) {
    return existingWafIntegrations.stream()
        .filter(
            wafIntegration ->
                wafIntegration
                        .getWafIntegrationDetails()
                        .getIntegrationParamsCase()
                        .equals(integrationParamsCase)
                    && !wafIntegration.getId().equals(id))
        .collect(Collectors.toUnmodifiableList());
  }

  private void validateUniqueIntegrationName(
      String name,
      List<WafIntegration> otherExistingWafIntegrationsOfSameType,
      WafIntegrationDetails.IntegrationParamsCase wafIntegrationType) {
    otherExistingWafIntegrationsOfSameType.forEach(
        wafIntegration -> {
          if (wafIntegration.getWafIntegrationDetails().getName().equals(name)) {
            throw Status.ALREADY_EXISTS
                .withDescription(wafIntegrationType + " + with name " + name + " already exists.")
                .asRuntimeException();
          }
        });
  }

  private void validateUpdatedF5IntegrationParams(
      F5IntegrationUpdateParams updatedF5IntegrationParams,
      List<WafIntegration> otherExistingWafIntegrations) {
    validateUpdatedF5IntegrationDetails(updatedF5IntegrationParams.getF5IntegrationDetails());
    validateF5IntegrationDetailsNoDuplicatesOrThrow(
        updatedF5IntegrationParams.getF5IntegrationDetails(), otherExistingWafIntegrations);
  }

  private void validateUpdatedAkamaiIntegrationParams(
      AkamaiIntegrationUpdateParams akamaiIntegrationUpdateParams,
      List<WafIntegration> otherExistingWafIntegrations) {
    validateUpdatedAkamaiIntegrationDetails(
        akamaiIntegrationUpdateParams.getAkamaiIntegrationDetails());
    validateAkamaiIntegrationDetailsNoDuplicatesOrThrow(
        akamaiIntegrationUpdateParams.getAkamaiIntegrationDetails(), otherExistingWafIntegrations);
  }

  private void validateUpdatedGcpIntegrationParams(
      GcpIntegrationUpdateParams gcpIntegrationUpdateParams,
      List<WafIntegration> otherExistingWafIntegrations) {
    validateUpdatedGcpIntegrationDetails(gcpIntegrationUpdateParams.getGcpIntegrationDetails());
    validateGcpIntegrationDetailsNoDuplicatesOrThrow(
        gcpIntegrationUpdateParams.getGcpIntegrationDetails(), otherExistingWafIntegrations);
  }

  private void validateUpdatedGcpIntegrationDetails(GcpIntegrationDetails gcpIntegrationDetails) {
    validateNonDefaultPresenceOrThrow(
        gcpIntegrationDetails, GcpIntegrationDetails.PROJECT_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        gcpIntegrationDetails, GcpIntegrationDetails.SECURITY_POLICY_NAME_FIELD_NUMBER);
    validateUpdatedGcpAuthCredentials(gcpIntegrationDetails.getAuthCredentials());
    validateSecurityPolicyScope(gcpIntegrationDetails);
  }

  private void validateGcpIntegrationDetailsNoDuplicatesOrThrow(
      GcpIntegrationDetails gcpIntegrationDetails,
      List<WafIntegration> otherExistingGCPWafIntegrations) {
    otherExistingGCPWafIntegrations.forEach(
        wafIntegration -> throwIfDuplicateParams(wafIntegration, gcpIntegrationDetails));
  }

  private void validateUpdatedF5IntegrationDetails(F5IntegrationDetails f5IntegrationDetails) {
    validateNonDefaultPresenceOrThrow(f5IntegrationDetails, F5IntegrationDetails.URL_FIELD_NUMBER);
    validateF5SecurityPolicyDetails(f5IntegrationDetails.getF5PolicyDetails());
    if (f5IntegrationDetails.hasF5AuthCredentials()) {
      validateF5AuthCredentials(f5IntegrationDetails.getF5AuthCredentials());
    }
  }

  private void validateUpdatedAkamaiIntegrationDetails(
      AkamaiIntegrationDetails akamaiIntegrationDetails) {
    validateNonDefaultPresenceOrThrow(
        akamaiIntegrationDetails, AkamaiIntegrationDetails.HOST_FIELD_NUMBER);
    validateAkamaiPolicyDetails(akamaiIntegrationDetails.getAkamaiPolicyDetails());
    if (akamaiIntegrationDetails.hasAkamaiAuthCredentials()) {
      validateAkamaiAuthCredentials(akamaiIntegrationDetails.getAkamaiAuthCredentials());
    }
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
        existingWafIntegrations =
            getOtherWafIntegrationOfSameType(
                null, CLOUDFLARE_INTEGRATION_PARAMS, existingWafIntegrations);
        validateUniqueIntegrationName(
            wafIntegrationDetails.getName(),
            existingWafIntegrations,
            CLOUDFLARE_INTEGRATION_PARAMS);
        validateCloudFlareIntegrationParams(
            wafIntegrationDetails.getCloudflareIntegrationParams(), existingWafIntegrations);
        break;
      case AWS_INTEGRATION_PARAMS:
        existingWafIntegrations =
            getOtherWafIntegrationOfSameType(null, AWS_INTEGRATION_PARAMS, existingWafIntegrations);
        validateUniqueIntegrationName(
            wafIntegrationDetails.getName(), existingWafIntegrations, AWS_INTEGRATION_PARAMS);
        validateAwsIntegrationParams(
            wafIntegrationDetails.getAwsIntegrationParams(), existingWafIntegrations);
        break;
      case IMPERVA_INTEGRATION_PARAMS:
        existingWafIntegrations =
            getOtherWafIntegrationOfSameType(
                null, IMPERVA_INTEGRATION_PARAMS, existingWafIntegrations);
        validateUniqueIntegrationName(
            wafIntegrationDetails.getName(), existingWafIntegrations, IMPERVA_INTEGRATION_PARAMS);
        validateImpervaIntegrationParam(wafIntegrationDetails.getImpervaIntegrationParams());
        validateNonCustomSignatureIntegrationTargets(
            wafIntegrationDetails,
            WafIntegrationDetails.IntegrationParamsCase.IMPERVA_INTEGRATION_PARAMS);
        break;
      case AZURE_INTEGRATION_PARAMS:
        existingWafIntegrations =
            getOtherWafIntegrationOfSameType(
                null, AZURE_INTEGRATION_PARAMS, existingWafIntegrations);
        validateUniqueIntegrationName(
            wafIntegrationDetails.getName(), existingWafIntegrations, AZURE_INTEGRATION_PARAMS);
        validateAzureIntegrationParam(wafIntegrationDetails.getAzureIntegrationParams());
        validateNonCustomSignatureIntegrationTargets(
            wafIntegrationDetails,
            WafIntegrationDetails.IntegrationParamsCase.AZURE_INTEGRATION_PARAMS);
        break;
      case GCP_INTEGRATION_PARAMS:
        existingWafIntegrations =
            getOtherWafIntegrationOfSameType(null, GCP_INTEGRATION_PARAMS, existingWafIntegrations);
        validateUniqueIntegrationName(
            wafIntegrationDetails.getName(), existingWafIntegrations, GCP_INTEGRATION_PARAMS);
        validateGcpIntegrationParams(
            wafIntegrationDetails.getGcpIntegrationParams(), existingWafIntegrations);
        validateNonCustomSignatureIntegrationTargets(
            wafIntegrationDetails,
            WafIntegrationDetails.IntegrationParamsCase.GCP_INTEGRATION_PARAMS);
        break;
      case F5_INTEGRATION_PARAMS:
        existingWafIntegrations =
            getOtherWafIntegrationOfSameType(null, F5_INTEGRATION_PARAMS, existingWafIntegrations);
        validateUniqueIntegrationName(
            wafIntegrationDetails.getName(), existingWafIntegrations, F5_INTEGRATION_PARAMS);
        validateF5IntegrationParams(
            wafIntegrationDetails.getF5IntegrationParams(), existingWafIntegrations);
        break;
      case AKAMAI_INTEGRATION_PARAMS:
        existingWafIntegrations =
            getOtherWafIntegrationOfSameType(
                null, AKAMAI_INTEGRATION_PARAMS, existingWafIntegrations);
        validateUniqueIntegrationName(
            wafIntegrationDetails.getName(), existingWafIntegrations, AKAMAI_INTEGRATION_PARAMS);
        validateAkamaiIntegrationParams(
            wafIntegrationDetails.getAkamaiIntegrationParams(), existingWafIntegrations);
        break;
      case INTEGRATIONPARAMS_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Unexpected integration params case: " + printMessage(wafIntegrationDetails))
            .asRuntimeException();
    }
  }

  private void validateGcpIntegrationParams(
      GcpIntegrationParams gcpIntegrationParams,
      List<WafIntegration> otherExistingGCPWafIntegrations) {
    validateGcpIntegrationDetails(
        gcpIntegrationParams.getGcpIntegrationDetails(), otherExistingGCPWafIntegrations);
  }

  private void validateF5IntegrationParams(
      F5IntegrationParams f5IntegrationParams,
      List<WafIntegration> otherExistingF5WafIntegrations) {
    validateF5IntegrationDetails(f5IntegrationParams.getF5IntegrationDetails());
    validateF5IntegrationDetailsNoDuplicatesOrThrow(
        f5IntegrationParams.getF5IntegrationDetails(), otherExistingF5WafIntegrations);
  }

  private void validateAkamaiIntegrationParams(
      AkamaiIntegrationParams akamaiIntegrationParams,
      List<WafIntegration> existingAkamaiWafIntegrations) {
    validateAkamaiIntegrationDetails(akamaiIntegrationParams.getAkamaiIntegrationDetails());
    validateAkamaiIntegrationDetailsNoDuplicatesOrThrow(
        akamaiIntegrationParams.getAkamaiIntegrationDetails(), existingAkamaiWafIntegrations);
  }

  private void validateGcpIntegrationDetails(
      GcpIntegrationDetails gcpIntegrationDetails,
      List<WafIntegration> otherExistingGCPWafIntegrations) {
    validateNonDefaultPresenceOrThrow(
        gcpIntegrationDetails, GcpIntegrationDetails.PROJECT_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        gcpIntegrationDetails, GcpIntegrationDetails.SECURITY_POLICY_NAME_FIELD_NUMBER);
    validateGcpAuthCredentials(gcpIntegrationDetails.getAuthCredentials());
    validateSecurityPolicyScope(gcpIntegrationDetails);
    validateGcpIntegrationDetailsNoDuplicatesOrThrow(
        gcpIntegrationDetails, otherExistingGCPWafIntegrations);
  }

  private void validateF5IntegrationDetails(F5IntegrationDetails f5IntegrationDetails) {
    validateNonDefaultPresenceOrThrow(f5IntegrationDetails, F5IntegrationDetails.URL_FIELD_NUMBER);
    validateUrlOrThrow(f5IntegrationDetails.getUrl());
    validateF5SecurityPolicyDetails(f5IntegrationDetails.getF5PolicyDetails());
    validateF5AuthCredentials(f5IntegrationDetails.getF5AuthCredentials());
  }

  private void validateAkamaiIntegrationDetails(AkamaiIntegrationDetails akamaiIntegrationDetails) {
    validateNonDefaultPresenceOrThrow(
        akamaiIntegrationDetails, AkamaiIntegrationDetails.HOST_FIELD_NUMBER);
    validateAkamaiPolicyDetails(akamaiIntegrationDetails.getAkamaiPolicyDetails());
    validateAkamaiAuthCredentials(akamaiIntegrationDetails.getAkamaiAuthCredentials());
  }

  private void validateUrlOrThrow(String urlString) {
    try {
      URL url = new URL(urlString);
      String protocol = url.getProtocol();
      if (!protocol.equals("https")) {
        throw Status.INVALID_ARGUMENT
            .withDescription("URL configured is not https.")
            .asRuntimeException();
      }
    } catch (MalformedURLException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription("URL configured is malformed.")
          .asRuntimeException();
    }
  }

  /** wafIntegrationId can be null while validating policy details for a creation request */
  private void validateF5SecurityPolicyDetails(F5PolicyDetails f5PolicyDetails) {
    validateNonDefaultPresenceOrThrow(f5PolicyDetails, F5PolicyDetails.POLICY_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(f5PolicyDetails, F5PolicyDetails.POLICY_NAME_FIELD_NUMBER);
  }

  private void validateAkamaiPolicyDetails(AkamaiPolicyDetails akamaiPolicyDetails) {
    validateNonDefaultPresenceOrThrow(
        akamaiPolicyDetails, AkamaiPolicyDetails.POLICY_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        akamaiPolicyDetails, AkamaiPolicyDetails.AKAMAI_POLICY_CONFIGURATION_ID_FIELD_NUMBER);
  }

  private void validateF5IntegrationDetailsNoDuplicatesOrThrow(
      F5IntegrationDetails f5IntegrationDetails,
      List<WafIntegration> otherExistingF5WafIntegrations) {
    otherExistingF5WafIntegrations.forEach(
        wafIntegration -> throwIfDuplicateParams(wafIntegration, f5IntegrationDetails));
  }

  private void validateAkamaiIntegrationDetailsNoDuplicatesOrThrow(
      AkamaiIntegrationDetails akamaiIntegrationDetails,
      List<WafIntegration> existingAkamaiWafIntegrations) {
    existingAkamaiWafIntegrations.forEach(
        wafIntegration -> throwIfDuplicateParams(wafIntegration, akamaiIntegrationDetails));
  }

  private void throwIfDuplicateParams(
      WafIntegration existingIntegration, F5IntegrationDetails f5IntegrationDetails) {
    F5IntegrationDetails existingDetails =
        existingIntegration
            .getWafIntegrationDetails()
            .getF5IntegrationParams()
            .getF5IntegrationDetails();

    if (existingDetails.getUrl().equals(f5IntegrationDetails.getUrl())
        && existingDetails
            .getF5PolicyDetails()
            .getPolicyName()
            .equals(f5IntegrationDetails.getF5PolicyDetails().getPolicyName())) {
      throw Status.ALREADY_EXISTS
          .withDescription(
              "F5 Application policy name "
                  + f5IntegrationDetails.getF5PolicyDetails().getPolicyName()
                  + " and server URL "
                  + f5IntegrationDetails.getUrl()
                  + " are already linked to an existing F5 WAF integration.")
          .asRuntimeException();
    }
  }

  private void throwIfDuplicateParams(
      WafIntegration existingIntegration, AkamaiIntegrationDetails akamaiIntegrationDetails) {
    AkamaiIntegrationDetails existingDetails =
        existingIntegration
            .getWafIntegrationDetails()
            .getAkamaiIntegrationParams()
            .getAkamaiIntegrationDetails();

    if (existingDetails
        .getAkamaiPolicyDetails()
        .getPolicyId()
        .equals(akamaiIntegrationDetails.getAkamaiPolicyDetails().getPolicyId())) {
      throw Status.ALREADY_EXISTS
          .withDescription(
              "Akamai policy id "
                  + akamaiIntegrationDetails.getAkamaiPolicyDetails().getPolicyId()
                  + " is already linked to an existing Akamai WAF integration.")
          .asRuntimeException();
    }
  }

  private void throwIfDuplicateParams(
      WafIntegration existingIntegration, GcpIntegrationDetails gcpIntegrationDetails) {
    GcpIntegrationDetails existingDetails =
        existingIntegration
            .getWafIntegrationDetails()
            .getGcpIntegrationParams()
            .getGcpIntegrationDetails();
    if (existingDetails.getProjectId().equals(gcpIntegrationDetails.getProjectId())
        && existingDetails
            .getSecurityPolicyName()
            .equals(gcpIntegrationDetails.getSecurityPolicyName())) {
      throw Status.ALREADY_EXISTS
          .withDescription(
              "Security policy name "
                  + gcpIntegrationDetails.getSecurityPolicyName()
                  + " and project ID "
                  + gcpIntegrationDetails.getProjectId()
                  + " are already linked to an existing GCP WAF Integration")
          .asRuntimeException();
    }
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

  private void validateF5AuthCredentials(F5AuthCredentials f5AuthCredentials) {
    validateNonDefaultPresenceOrThrow(
        f5AuthCredentials, F5AuthCredentials.ENCRYPTED_USER_NAME_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        f5AuthCredentials, F5AuthCredentials.ENCRYPTED_PASSWORD_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        f5AuthCredentials, F5AuthCredentials.ENCRYPTION_KEY_ID_FIELD_NUMBER);
  }

  private void validateAkamaiAuthCredentials(AkamaiAuthCredentials akamaiAuthCredentials) {
    validateNonDefaultPresenceOrThrow(
        akamaiAuthCredentials, AkamaiAuthCredentials.ENCRYPTION_KEY_ID_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        akamaiAuthCredentials, AkamaiAuthCredentials.ENCRYPTED_ACCESS_TOKEN_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        akamaiAuthCredentials, AkamaiAuthCredentials.ENCRYPTED_CLIENT_TOKEN_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        akamaiAuthCredentials, AkamaiAuthCredentials.ENCRYPTED_CLIENT_SECRET_FIELD_NUMBER);
  }

  private void validateUpdatedCloudFlareIntegrationParams(
      UpdatedCloudflareIntegrationParams cloudflareIntegrationParams,
      List<WafIntegration> otherExistingCloudFlareWafIntegrations) {
    validateNonDefaultPresenceOrThrow(
        cloudflareIntegrationParams, CloudflareIntegrationParams.ZONE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        cloudflareIntegrationParams, CloudflareIntegrationParams.EMAIL_FIELD_NUMBER);
    if (cloudflareIntegrationParams.hasEncryptedApiToken()) {
      this.validateEncryptedData(cloudflareIntegrationParams.getEncryptedApiToken());
    }
    validateZoneDoesntExist(
        cloudflareIntegrationParams.getZone(), otherExistingCloudFlareWafIntegrations);
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
      List<WafIntegration> otherExistingAWSWafIntegrations) {
    validateNonDefaultPresenceOrThrow(
        awsIntegrationUpdateParams, AwsIntegrationUpdateParams.RESOURCES_FIELD_NUMBER);
    awsIntegrationUpdateParams.getResourcesList().forEach(this::validateAwsResource);
    validateArnAlreadyExistInAwsWafIntegration(
        id, awsIntegrationUpdateParams.getResourcesList(), otherExistingAWSWafIntegrations);
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
      CloudflareIntegrationParams cloudflareIntegrationParams,
      List<WafIntegration> otherExistingCloudFlareWafIntegrations) {
    validateNonDefaultPresenceOrThrow(
        cloudflareIntegrationParams, CloudflareIntegrationParams.ZONE_FIELD_NUMBER);
    validateNonDefaultPresenceOrThrow(
        cloudflareIntegrationParams, CloudflareIntegrationParams.EMAIL_FIELD_NUMBER);
    this.validateEncryptedData(cloudflareIntegrationParams.getEncryptedApiToken());
    validateZoneDoesntExist(
        cloudflareIntegrationParams.getZone(), otherExistingCloudFlareWafIntegrations);
  }

  private void validateZoneDoesntExist(String zone, List<WafIntegration> existingIntegrations) {
    if (existingIntegrations.stream()
        .map(
            integration ->
                integration.getWafIntegrationDetails().getCloudflareIntegrationParams().getZone())
        .anyMatch(existingZone -> existingZone.equals(zone))) {
      throw Status.INVALID_ARGUMENT
          .withDescription("integration with the given zone already exists")
          .asRuntimeException();
    }
  }

  private void validateAwsIntegrationParams(
      AwsIntegrationParams awsIntegrationParams,
      List<WafIntegration> otherExistingAWSWafIntegrations) {
    validateCredentials(awsIntegrationParams);
    validateNonDefaultPresenceOrThrow(
        awsIntegrationParams, AwsIntegrationParams.RESOURCES_FIELD_NUMBER);
    awsIntegrationParams.getResourcesList().forEach(this::validateAwsResource);
    validateArnAlreadyExistInAwsWafIntegration(
        awsIntegrationParams.getResourcesList(), otherExistingAWSWafIntegrations);
  }

  private static void validateArnAlreadyExistInAwsWafIntegration(
      List<AwsResource> resources, List<WafIntegration> otherExistingAWSWafIntegrations) {
    otherExistingAWSWafIntegrations.forEach(
        wafIntegration -> validateArn(resources, wafIntegration));
  }

  private void validateArnAlreadyExistInAwsWafIntegration(
      String id,
      List<AwsResource> resources,
      List<WafIntegration> otherExistingAWSWafIntegrations) {
    otherExistingAWSWafIntegrations.stream()
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
