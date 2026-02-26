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
import java.util.stream.Collectors;
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

    if (!filter.getIssueConfigurationIdsList().isEmpty()) {
      filters.add(
          config -> filter.getIssueConfigurationIdsList().contains(config.getConfigurationId()));
    }

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

  List<JiraTemplate> getJiraTemplates(
      RequestContext requestContext,
      String templateId,
      List<TraceableEntityType> entityTypes,
      String prefix) {
    List<Function<JiraTemplate, Boolean>> filters = new ArrayList<>();

    if (templateId != null && !templateId.isEmpty()) {
      filters.add(template -> template.getTemplateId().equals(templateId));
    }

    if (entityTypes != null && !entityTypes.isEmpty()) {
      List<TraceableEntityType> normalizedEntityTypes = normalizeEntityTypes(entityTypes);
      filters.add(template -> normalizedEntityTypes.contains(template.getEntityType()));
    }

    if (prefix != null && !prefix.isEmpty()) {
      filters.add(template -> template.getJiraTemplateDetails().getName().startsWith(prefix));
    }

    return this.getAllConfigData(requestContext).stream()
        .flatMap(
            config ->
                config.getJiraProjectIssueConfigurationDetails().getJiraTemplateList().stream())
        .filter(
            template -> filters.stream().allMatch(filterFunction -> filterFunction.apply(template)))
        .collect(Collectors.toList());
  }

  private List<TraceableEntityType> normalizeEntityTypes(List<TraceableEntityType> entityTypes) {
    List<TraceableEntityType> expandedTypes = new ArrayList<>();
    for (TraceableEntityType entityType : entityTypes) {
      expandedTypes.add(entityType);
      if (entityType == TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY
          || entityType == TraceableEntityType.TRACEABLE_ENTITY_TYPE_VULNERABILITY) {
        expandedTypes.add(TraceableEntityType.TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY);
        expandedTypes.add(TraceableEntityType.TRACEABLE_ENTITY_TYPE_VULNERABILITY);
      }
    }
    return expandedTypes.stream().distinct().collect(Collectors.toList());
  }
}
