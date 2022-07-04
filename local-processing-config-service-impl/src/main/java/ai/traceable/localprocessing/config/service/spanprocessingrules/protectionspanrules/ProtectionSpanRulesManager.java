package ai.traceable.localprocessing.config.service.spanprocessingrules.protectionspanrules;

import ai.traceable.localprocessing.config.service.v1.ProtectionSpanProcessingRule;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ProtectionSpanRulesManager {
  List<ProtectionSpanProcessingRule> getAllProtectionSpanProcessingRules(
      RequestContext requestContext, String serviceName, Optional<String> environment);
}
