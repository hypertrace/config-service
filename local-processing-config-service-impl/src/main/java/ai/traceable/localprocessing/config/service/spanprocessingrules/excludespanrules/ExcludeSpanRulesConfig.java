package ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules;

import com.typesafe.config.Config;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;

@Getter
public class ExcludeSpanRulesConfig {

  private static final String AGENT_UNSUPPORTED_RULE_IDS_PATH =
      "span.exclusion.rules.agent.unsupported.rule.ids";

  private final Set<String> agentUnsupportedRuleIds;

  public ExcludeSpanRulesConfig(Config config) {
    this.agentUnsupportedRuleIds = readAgentUnsupportedRuleIds(config);
  }

  private Set<String> readAgentUnsupportedRuleIds(Config config) {
    return config.getStringList(AGENT_UNSUPPORTED_RULE_IDS_PATH).stream()
        .collect(Collectors.toUnmodifiableSet());
  }
}
