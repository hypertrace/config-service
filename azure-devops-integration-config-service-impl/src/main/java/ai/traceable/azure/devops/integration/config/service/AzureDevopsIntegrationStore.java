package ai.traceable.azure.devops.integration.config.service;

import ai.traceable.azure.devops.integration.config.service.api.v1.AzureDevopsIntegrationFilter;
import ai.traceable.azure.devops.integration.config.service.api.v1.AzureDevopsIntegrationWithAuthCredentials;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

class AzureDevopsIntegrationStore
    extends IdentifiedObjectStoreWithFilter<
        AzureDevopsIntegrationWithAuthCredentials, AzureDevopsIntegrationFilter> {
  private static final String RESOURCE_NAMESPACE = "azureDevopsIntegrationsConfig";
  private static final String RESOURCE_NAME = "azureDevopsIntegrations";

  @Inject
  public AzureDevopsIntegrationStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(configServiceBlockingStub, RESOURCE_NAMESPACE, RESOURCE_NAME, configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<AzureDevopsIntegrationWithAuthCredentials> buildDataFromValue(Value value) {
    AzureDevopsIntegrationWithAuthCredentials.Builder builder =
        AzureDevopsIntegrationWithAuthCredentials.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(AzureDevopsIntegrationWithAuthCredentials data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(AzureDevopsIntegrationWithAuthCredentials data) {
    return data.getId();
  }

  @Override
  protected Optional<AzureDevopsIntegrationWithAuthCredentials> filterConfigData(
      AzureDevopsIntegrationWithAuthCredentials data, AzureDevopsIntegrationFilter filter) {
    return Optional.of(data)
        .filter(
            azureDevopsIntegration ->
                !filter.hasScope()
                    || !data.getIntegrationDetails().hasAzureDevopsIntegrationScope()
                    || filter.getScope().getEnvironmentIds().getValuesList().stream()
                        .anyMatch(
                            environment ->
                                data.getIntegrationDetails()
                                    .getAzureDevopsIntegrationScope()
                                    .getEnvironmentIds()
                                    .getValuesList()
                                    .contains(environment)));
  }
}
