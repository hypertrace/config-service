package ai.traceable.jira.integration.config.service;

import ai.traceable.jira.integration.config.service.api.v1.JiraIntegration;
import ai.traceable.jira.integration.config.service.api.v1.JiraIntegrationFilter;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;

class JiraIntegrationStore
    extends IdentifiedObjectStoreWithFilter<JiraIntegration, JiraIntegrationFilter> {
  private static final String RESOURCE_NAMESPACE = "jiraIntegrationsConfig";
  private static final String RESOURCE_NAME = "jiraIntegrations";

  @Inject
  public JiraIntegrationStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(configServiceBlockingStub, RESOURCE_NAMESPACE, RESOURCE_NAME, configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<JiraIntegration> buildDataFromValue(Value value) {
    JiraIntegration.Builder builder = JiraIntegration.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(JiraIntegration data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(JiraIntegration data) {
    return data.getId();
  }

  @Override
  protected Optional<JiraIntegration> filterConfigData(
      JiraIntegration data, JiraIntegrationFilter filter) {
    return Optional.of(data)
        .filter(
            jiraIntegration ->
                !filter.hasFilterScope()
                    || !data.hasScope()
                    || filter.getFilterScope().getEnvironmentIds().getValuesList().stream()
                        .anyMatch(
                            environment ->
                                data.getScope()
                                    .getEnvironmentIds()
                                    .getValuesList()
                                    .contains(environment)));
  }
}
