package ai.traceable.malicioussources.config.service.rules;

import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesManager {
  MaliciousSourcesRule createMaliciousSourcesRule(
      RequestContext requestContext, CreateMaliciousSourcesRuleRequest createRuleRequest);
}
