package ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules;

import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRule;
import java.util.List;
import java.util.Optional;

public interface ExcludeSpanRulesManager {
  List<ExcludeSpanProcessingRule> getAllExcludeSpanProcessingRules(
      String serviceName, Optional<String> environment);
}
