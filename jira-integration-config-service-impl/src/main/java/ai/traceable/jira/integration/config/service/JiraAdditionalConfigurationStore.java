package ai.traceable.jira.integration.config.service;

import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsFilter;
import ai.traceable.jira.integration.config.service.api.v1.JiraProjectIssueConfiguration;
import ai.traceable.jira.integration.config.service.api.v1.JiraTemplate;
import ai.traceable.jira.integration.config.service.api.v1.TraceableEntityType;
import com.google.inject.Inject;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

class JiraAdditionalConfigurationStore
    extends IdentifiedObjectStoreWithFilter<
        JiraProjectIssueConfiguration, GetProjectIssueConfigurationsFilter> {
  private static final String RESOURCE_NAMESPACE = "jiraIntegrationsConfig";
  private static final String RESOURCE_NAME = "jiraIntegrationAdditionalConfiguration";

  @Inject
  public JiraAdditionalConfigurationStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(configServiceBlockingStub, RESOURCE_NAMESPACE, RESOURCE_NAME, configChangeEventGenerator);
  }

  @SneakyThrows
  @Override
  protected Optional<JiraProjectIssueConfiguration> buildDataFromValue(Value value) {
    JiraProjectIssueConfiguration.Builder builder = JiraProjectIssueConfiguration.newBuilder();
    ConfigProtoConverter.mergeFromValue(value, builder);
    return Optional.of(builder.build());
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(JiraProjectIssueConfiguration data) {
    return ConfigProtoConverter.convertToValue(data);
  }

  @Override
  protected String getContextFromData(JiraProjectIssueConfiguration data) {
    return data.getConfigurationId();
  }

  @Override
  protected Optional<JiraProjectIssueConfiguration> filterConfigData(
      JiraProjectIssueConfiguration data, GetProjectIssueConfigurationsFilter filter) {
    List<Function<JiraProjectIssueConfiguration, Boolean>> filters = new ArrayList<>();

    if (!filter.getIntegrationIdsList().isEmpty()) {
      filters.add(
          config ->
              filter
                  .getIntegrationIdsList()
                  .contains(config.getJiraProjectIssueConfigurationDetails().getIntegrationId()));
    }

    if (filter.hasProjectId()) {
      filters.add(
          config ->
              config
                  .getJiraProjectIssueConfigurationDetails()
                  .getProjectId()
                  .equals(filter.getProjectId()));
    }

    if (filter.hasIssueType()) {
      filters.add(
          config ->
              config
                  .getJiraProjectIssueConfigurationDetails()
                  .getIssueType()
                  .equals(filter.getIssueType()));
    }

    for (TraceableEntityType entityType : filter.getSupportedEntityTypesList()) {
      filters.add(
          config ->
              config
                  .getJiraProjectIssueConfigurationDetails()
                  .getValidTraceableEntityType()
                  .equals(entityType));
    }

    return Optional.of(data)
        .filter(
            config -> filters.stream().allMatch(filterFunction -> filterFunction.apply(config)));
  }

  Optional<JiraTemplate> getJiraTemplate(RequestContext requestContext, String templateId) {
    return this.getAllConfigData(requestContext).stream()
        .flatMap(
            config ->
                config.getJiraProjectIssueConfigurationDetails().getJiraTemplateList().stream())
        .filter(jiraTemplate -> jiraTemplate.getTemplateId().equals(templateId))
        .findAny();
  }
}
