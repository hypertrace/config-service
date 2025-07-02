package ai.traceable.genai.system.discovery.info;

import ai.traceable.genai.system.discovery.config.service.v1.GenAiSystemDiscoveryRule;
import java.util.Map;
import lombok.Value;

@Value
public class GenAiSystemDiscoveryConfig {
  Map<String, GenAiSystemDiscoveryRule> genAiRuleIdToGenAiSystemDiscoveryRuleMap;
}
