package ai.traceable.waf.provider.integration.service;

import ai.traceable.waf.integration.service.api.v1.GetWafIntegrationsFilter;
import ai.traceable.waf.integration.service.api.v1.WafIntegration;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationDetails;
import ai.traceable.waf.integration.service.api.v1.WafIntegrationScope;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class WafIntegrationStore
    extends IdentifiedObjectStoreWithFilter<WafIntegration, GetWafIntegrationsFilter> {
  private static final String WAF_INTEGRATION_CONFIG_RESOURCE_NAME = "waf-integration-config";
  private static final String WAF_INTEGRATION_CONFIG_RESOURCE_NAMESPACE =
      "waf-provider-integration";

  @Inject
  WafIntegrationStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        WAF_INTEGRATION_CONFIG_RESOURCE_NAMESPACE,
        WAF_INTEGRATION_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  public List<WafIntegration> getAllConfigData(
      RequestContext context, GetWafIntegrationsFilter filter) {
    List<WafIntegration> wafIntegrations = super.getAllConfigData(context, filter);
    return wafIntegrations.stream()
        .filter(rule -> filterConfigData(rule, filter).isPresent())
        .collect(Collectors.toList());
  }

  @Override
  protected Optional<WafIntegration> buildDataFromValue(Value value) {
    try {
      WafIntegration.Builder builder = WafIntegration.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(
          WafIntegrationBuilderUtils.getBackwardCompatibleWafIntegration(builder.build()));
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(WafIntegration object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromData(WafIntegration object) {
    return object.getId();
  }

  @Override
  protected Optional<WafIntegration> filterConfigData(
      WafIntegration data, GetWafIntegrationsFilter filter) {
    List<GetWafIntegrationsFilter.WafProviderType> requiredTypes = filter.getWafProviderTypesList();
    List<String> requiredIds = filter.getIdsList();
    return Optional.of(data)
        .filter(wafIntegration -> checkWafIdPresence(wafIntegration.getId(), requiredIds))
        .filter(
            wafIntegration ->
                checkWafTypePresence(
                    getWafProviderTypeFromDetails(wafIntegration.getWafIntegrationDetails()),
                    requiredTypes))
        .filter(
            wafIntegration ->
                filterOnScope(
                    wafIntegration.getWafIntegrationDetails().getWafIntegrationScope(),
                    filter.getWafIntegrationScope()));
  }

  private boolean filterOnScope(
      WafIntegrationScope wafIntegrationScope, WafIntegrationScope wafIntegrationFilterScope) {
    List<String> environmentIds = wafIntegrationScope.getEnvironmentScope().getEnvironmentIdsList();
    if (!wafIntegrationFilterScope.hasEnvironmentScope() || environmentIds.isEmpty()) {
      return true;
    }

    List<String> filterEnvironmentIds =
        wafIntegrationFilterScope.getEnvironmentScope().getEnvironmentIdsList();
    return environmentIds.stream().anyMatch(filterEnvironmentIds::contains);
  }

  private boolean checkWafIdPresence(String id, List<String> requiredIds) {
    // empty list is treated as no filter
    if (requiredIds.isEmpty()) {
      return true;
    }
    return requiredIds.contains(id);
  }

  private boolean checkWafTypePresence(
      GetWafIntegrationsFilter.WafProviderType type,
      List<GetWafIntegrationsFilter.WafProviderType> requiredTypes) {
    if (requiredTypes.isEmpty()) {
      return true;
    }
    return requiredTypes.contains(type);
  }

  private GetWafIntegrationsFilter.WafProviderType getWafProviderTypeFromDetails(
      WafIntegrationDetails details) {
    switch (details.getIntegrationParamsCase()) {
      case CLOUDFLARE_INTEGRATION_PARAMS:
        return GetWafIntegrationsFilter.WafProviderType.WAF_PROVIDER_TYPE_CLOUDFLARE;
      case AWS_INTEGRATION_PARAMS:
        return GetWafIntegrationsFilter.WafProviderType.WAF_PROVIDER_TYPE_AWS;
      case IMPERVA_INTEGRATION_PARAMS:
        return GetWafIntegrationsFilter.WafProviderType.WAF_PROVIDER_TYPE_IMPERVA;
      case AZURE_INTEGRATION_PARAMS:
        return GetWafIntegrationsFilter.WafProviderType.WAF_PROVIDER_TYPE_AZURE;
      case GCP_INTEGRATION_PARAMS:
        return GetWafIntegrationsFilter.WafProviderType.WAF_PROVIDER_TYPE_GCP;
      case F5_INTEGRATION_PARAMS:
        return GetWafIntegrationsFilter.WafProviderType.WAF_PROVIDER_TYPE_F5;
      case AKAMAI_INTEGRATION_PARAMS:
        return GetWafIntegrationsFilter.WafProviderType.WAF_PROVIDER_TYPE_AKAMAI;
      case FORTINET_INTEGRATION_PARAMS:
        return GetWafIntegrationsFilter.WafProviderType.WAF_PROVIDER_TYPE_FORTINET;
      case INTEGRATIONPARAMS_NOT_SET:
      default:
        return GetWafIntegrationsFilter.WafProviderType.WAF_PROVIDER_TYPE_UNSPECIFIED;
    }
  }
}
