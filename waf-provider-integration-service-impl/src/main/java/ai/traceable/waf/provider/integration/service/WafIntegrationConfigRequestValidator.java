package ai.traceable.waf.provider.integration.service;

import static org.hypertrace.config.validation.GrpcValidatorUtils.printMessage;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateNonDefaultPresenceOrThrow;
import static org.hypertrace.config.validation.GrpcValidatorUtils.validateRequestContextOrThrow;

import ai.traceable.waf.integration.service.api.v1.CloudflareIntegrationParams;
import ai.traceable.waf.integration.service.api.v1.CreateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.DeleteWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter.WafProviderType;
import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsRequest;
import ai.traceable.waf.integration.service.api.v1.UpdateWafIntegrationRequest;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails;
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
    validateWafIntegrationDetails(request.getWafIntegrationDetails());
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

  private void validateWafIntegrationDetails(WafIntegrationDetails wafIntegrationDetails) {
    validateNonDefaultPresenceOrThrow(
        wafIntegrationDetails, WafIntegrationDetails.NAME_FIELD_NUMBER);
    validateIntegrationParams(wafIntegrationDetails);
  }

  private void validateWafProviderType(WafProviderType type) {
    switch (type) {
      case WAF_PROVIDER_TYPE_CLOUDFLARE:
        break;
      case WAF_PROVIDER_TYPE_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unexpected waf provider type case: " + type)
            .asRuntimeException();
    }
  }

  private void validateIntegrationParams(WafIntegrationDetails wafIntegrationDetails) {
    switch (wafIntegrationDetails.getIntegrationParamsCase()) {
      case CLOUDFLARE_INTEGRATION_PARAMS:
        validateCloudFlareIntegrationParams(wafIntegrationDetails.getCloudflareIntegrationParams());
        break;
      case INTEGRATIONPARAMS_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription(
                "Unexpected integration params case: " + printMessage(wafIntegrationDetails))
            .asRuntimeException();
    }
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
}
