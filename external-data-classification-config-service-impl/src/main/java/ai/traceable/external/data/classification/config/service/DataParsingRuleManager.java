package ai.traceable.external.data.classification.config.service;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.external.data.classification.config.service.v1.DataParsingRule;
import ai.traceable.external.data.classification.config.service.v1.DataParsingRule.DataParsingMode;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.AgentCapabilities;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = @Inject)
class DataParsingRuleManager {
  ExternalDataClassificationConfig config;
  AgentVersionManager agentVersionManager;
  FeatureCachingClient featureCachingClient;

  List<DataParsingRule> getDefaultParsingRulesForAgent(
      RequestContext requestContext, AgentCapabilities agentCapabilities) {
    List<DataParsingRule> dataParsingRules = new ArrayList<>();
    if (this.agentVersionManager.isSseDataParsingRulesSupported(agentCapabilities)
        && this.featureCachingClient.isGenAiDetectionV2Enabled(requestContext)) {
      dataParsingRules.addAll(config.getDefaultSseDataParsingRules());
    }
    if (!this.agentVersionManager.isContentTypeParseRuleSupported(agentCapabilities)) {
      config.getDefaultDataParsingRules().stream()
          .filter(Predicate.not(this::isContentTypeRule))
          .forEach(dataParsingRules::add);
      return dataParsingRules;
    }
    dataParsingRules.addAll(config.getDefaultDataParsingRules());
    return dataParsingRules;
  }

  private boolean isContentTypeRule(DataParsingRule rule) {
    return rule.getMode().equals(DataParsingMode.DATA_PARSING_MODE_CONTENT_TYPE_HEADER);
  }
}
