package ai.traceable.external.data.classification.config.service;

import ai.traceable.config.utils.SemanticVersioningComparator;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.AgentCapabilities;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.Component;
import jakarta.inject.Inject;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
class AgentVersionManager {
  private static final String CONTENT_TYPE_PARSE_RULE_SUPPORT_MIN_TPA_VERSION = "1.39.0-rc.0";
  SemanticVersioningComparator versionComparator;

  boolean isContentTypeParseRuleSupported(AgentCapabilities agentCapabilities) {
    return this.getTpaVersion(agentCapabilities)
        .map(
            version ->
                versionComparator.isVersionSupported(
                    version, CONTENT_TYPE_PARSE_RULE_SUPPORT_MIN_TPA_VERSION))
        .orElse(false);
  }

  private Optional<String> getTpaVersion(AgentCapabilities agentCapabilities) {
    return agentCapabilities.getComponentsList().stream()
        .filter(Component::hasTraceablePlatformAgentVersion)
        .map(Component::getTraceablePlatformAgentVersion)
        .findFirst();
  }
}
