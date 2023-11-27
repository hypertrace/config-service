package ai.traceable.external.data.classification.config.service;

import ai.traceable.external.data.classification.config.service.v1.DataParsingRule;
import ai.traceable.external.data.classification.config.service.v1.DataParsingRule.DataParsingMode;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.AgentCapabilities;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
class DataParsingRuleManager {
  ExternalDataClassificationConfig config;
  AgentVersionManager agentVersionManager;

  List<DataParsingRule> getDefaultParsingRulesForAgent(AgentCapabilities agentCapabilities) {
    if (!this.agentVersionManager.isContentTypeParseRuleSupported(agentCapabilities)) {
      return config.getDefaultDataParsingRules().stream()
          .filter(Predicate.not(this::isContentTypeRule))
          .collect(Collectors.toUnmodifiableList());
    }
    return config.getDefaultDataParsingRules();
  }

  private boolean isContentTypeRule(DataParsingRule rule) {
    return rule.getMode().equals(DataParsingMode.DATA_PARSING_MODE_CONTENT_TYPE_HEADER);
  }
}
