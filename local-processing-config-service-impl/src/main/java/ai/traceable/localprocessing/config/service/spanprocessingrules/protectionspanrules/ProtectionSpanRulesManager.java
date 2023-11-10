package ai.traceable.localprocessing.config.service.spanprocessingrules.protectionspanrules;

import ai.traceable.localprocessing.config.service.v1.ProtectionSpanProcessingRule;
import ai.traceable.span.processing.config.service.v1.ProtectionSpanRule;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ProtectionSpanRulesManager {
  List<ProtectionSpanRule> getAllProtectionSpanRules(RequestContext requestContext);

  List<ProtectionSpanProcessingRule> getAllMatchingProtectionSpanProcessingRules(
      RequestContext requestContext,
      List<ProtectionSpanRule> protectionSpanRules,
      String serviceName,
      Optional<String> environment);
}
