package ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules;

import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRule;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ExcludeSpanRulesManager {
  List<ExcludeSpanProcessingRule> getAllExcludeSpanProcessingRules(
      RequestContext requestContext, String serviceName, Optional<String> environment);
}
