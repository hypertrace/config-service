package ai.traceable.servicenow.itsm.integration.config.service;

import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationFilter;
import ai.traceable.servicenow.itsm.integration.config.service.api.v1.ServiceNowItsmIntegrationWithAuthCredentials;
import com.google.protobuf.Value;
import java.util.Optional;
import javax.inject.Inject;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

class ServiceNowItsmIntegrationStore
    extends IdentifiedObjectStoreWithFilter<
        ServiceNowItsmIntegrationWithAuthCredentials, ServiceNowItsmIntegrationFilter> {
  private static final String RESOURCE_NAMESPACE = "serviceNowItsmIntegrationsConfig";
  private static final String RESOURCE_NAME = "serviceNowItsmIntegrations";

  @Inject
  public ServiceNowItsmIntegrationStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(configServiceBlockingStub, RESOURCE_NAMESPACE, RESOURCE_NAME, configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<ServiceNowItsmIntegrationWithAuthCredentials> buildDataFromValue(Value value) {
    ServiceNowItsmIntegrationWithAuthCredentials.Builder builder =
        ServiceNowItsmIntegrationWithAuthCredentials.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(ServiceNowItsmIntegrationWithAuthCredentials data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(ServiceNowItsmIntegrationWithAuthCredentials data) {
    return data.getId();
  }

  @Override
  protected Optional<ServiceNowItsmIntegrationWithAuthCredentials> filterConfigData(
      ServiceNowItsmIntegrationWithAuthCredentials data, ServiceNowItsmIntegrationFilter filter) {
    return Optional.of(data)
        .filter(
            serviceNowItsmIntegration ->
                !filter.hasScope()
                    || !data.getIntegrationDetails().hasScope()
                    || filter.getScope().getEnvironmentIds().getValuesList().stream()
                        .anyMatch(
                            environment ->
                                data.getIntegrationDetails()
                                    .getScope()
                                    .getEnvironmentIds()
                                    .getValuesList()
                                    .contains(environment)));
  }
}
