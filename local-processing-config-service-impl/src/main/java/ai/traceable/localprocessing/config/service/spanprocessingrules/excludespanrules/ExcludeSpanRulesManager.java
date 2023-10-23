package ai.traceable.localprocessing.config.service.spanprocessingrules.excludespanrules;

import ai.traceable.localprocessing.config.service.v1.ExcludeSpanProcessingRule;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.span.processing.config.service.v1.ExcludeSpanRule;

public interface ExcludeSpanRulesManager {
  List<ExcludeSpanProcessingRule> getAllMatchingExcludeSpanProcessingRules(
      RequestContext requestContext,
      List<ExcludeSpanRule> excludeSpanRules,
      String serviceName,
      Optional<String> environment);

  List<ExcludeSpanRule> getAllExcludeSpanRules(RequestContext requestContext);
}
