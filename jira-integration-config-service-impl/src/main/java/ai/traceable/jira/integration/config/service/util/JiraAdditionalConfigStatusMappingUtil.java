package ai.traceable.jira.integration.config.service.util;

import ai.traceable.jira.integration.config.service.api.v1.GetProjectIssueConfigurationsFilter;
import ai.traceable.jira.integration.config.service.api.v1.JiraProjectIssueConfiguration;
import ai.traceable.jira.integration.config.service.api.v1.JiraStatusMapping;
import ai.traceable.jira.integration.config.service.api.v1.JiraStatusMappingConfiguration;
import ai.traceable.jira.integration.config.service.api.v1.TraceableEntityType;
import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;

@UtilityClass
public class JiraAdditionalConfigStatusMappingUtil {
  private final String COMMON_PREFIX = "TRACEABLE_ENTITY_STATUS_ISSUES_COMMON";
  private final String AST_VULNERABILITY_PREFIX = "TRACEABLE_ENTITY_STATUS_AST_VULNERABILITY";
  private final String VULNERABILITY_PREFIX = "TRACEABLE_ENTITY_STATUS_VULNERABILITY";
  private final String THREAT_ACTIVITY_PREFIX = "TRACEABLE_ENTITY_STATUS_THREAT_ACTIVITY";

  public List<JiraProjectIssueConfiguration> getFilteredConfiguration(
      GetProjectIssueConfigurationsFilter filter,
      List<JiraProjectIssueConfiguration> allConfigurations) {
    // Find specific prefix based on entity type provided
    List<String> specificPrefixes =
        filter.getSupportedEntityTypesList().stream()
            .map(JiraAdditionalConfigStatusMappingUtil::getPrefixForEntityType)
            .distinct()
            .collect(Collectors.toList());
    // Fallback to common prefix if entity-specific mappings are not present
    return allConfigurations.stream()
        .map(config -> filterStatusMappings(config, specificPrefixes))
        .filter(JiraAdditionalConfigStatusMappingUtil::hasNonEmptyStatusMappings)
        .collect(Collectors.toList());
  }

  private static String getPrefixForEntityType(TraceableEntityType entityType) {
    switch (entityType) {
      case TRACEABLE_ENTITY_TYPE_AST_VULNERABILITY:
        return AST_VULNERABILITY_PREFIX;
      case TRACEABLE_ENTITY_TYPE_VULNERABILITY:
        return VULNERABILITY_PREFIX;
      case TRACEABLE_ENTITY_TYPE_THREAT_ACTIVITY:
        return THREAT_ACTIVITY_PREFIX;
      default:
        return COMMON_PREFIX;
    }
  }

  private JiraProjectIssueConfiguration filterStatusMappings(
      JiraProjectIssueConfiguration config, List<String> specificPrefixes) {
    List<JiraStatusMapping> statusMappings =
        config
            .getJiraProjectIssueConfigurationDetails()
            .getJiraStatusMappingConfiguration()
            .getStatusMappingsList();
    List<JiraStatusMapping> specificMappings =
        filterBySpecificPrefixes(statusMappings, specificPrefixes);
    if (!specificMappings.isEmpty()) {
      return updateConfigurationWithFilteredMappings(config, specificMappings);
    } else {
      List<JiraStatusMapping> commonMappings = filterByCommonPrefix(statusMappings);
      return updateConfigurationWithFilteredMappings(config, commonMappings);
    }
  }

  private List<JiraStatusMapping> filterBySpecificPrefixes(
      List<JiraStatusMapping> statusMappings, List<String> specificPrefixes) {
    return statusMappings.stream()
        .filter(
            statusMapping ->
                specificPrefixes.stream()
                    .anyMatch(
                        prefix ->
                            statusMapping.getTraceableEntityStatus().name().startsWith(prefix)))
        .collect(Collectors.toList());
  }

  private List<JiraStatusMapping> filterByCommonPrefix(List<JiraStatusMapping> statusMappings) {
    return statusMappings.stream()
        .filter(
            statusMapping ->
                statusMapping.getTraceableEntityStatus().name().startsWith(COMMON_PREFIX))
        .collect(Collectors.toList());
  }

  private boolean hasNonEmptyStatusMappings(JiraProjectIssueConfiguration config) {
    return !config
        .getJiraProjectIssueConfigurationDetails()
        .getJiraStatusMappingConfiguration()
        .getStatusMappingsList()
        .isEmpty();
  }

  private JiraProjectIssueConfiguration updateConfigurationWithFilteredMappings(
      JiraProjectIssueConfiguration config, List<JiraStatusMapping> statusMappings) {
    return config.toBuilder()
        .setJiraProjectIssueConfigurationDetails(
            config.getJiraProjectIssueConfigurationDetails().toBuilder()
                .clearJiraStatusMappingConfiguration()
                .setJiraStatusMappingConfiguration(
                    JiraStatusMappingConfiguration.newBuilder()
                        .addAllStatusMappings(statusMappings)))
        .build();
  }
}
