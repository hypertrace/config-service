package ai.traceable.span.processing.config.service.store;

import ai.traceable.span.processing.config.service.v1.ApiNamingRuleDetails;
import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ApiNamingRulesResult {
  List<ApiNamingRuleDetails> ruleDetails;
  long totalCount;
}
