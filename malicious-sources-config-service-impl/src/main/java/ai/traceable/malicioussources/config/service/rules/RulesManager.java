package ai.traceable.malicioussources.config.service.rules;

import ai.traceable.malicioussources.config.service.v1.CreateMaliciousSourcesRuleRequest;
import ai.traceable.malicioussources.config.service.v1.GetRulesFilter;
import ai.traceable.malicioussources.config.service.v1.MaliciousSourcesRule;
import ai.traceable.malicioussources.config.service.v1.UpdateMaliciousSourcesRuleRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesManager {
  List<MaliciousSourcesRule> getMaliciousSourcesRules(
      RequestContext requestContext, GetRulesFilter filter);

  MaliciousSourcesRule createMaliciousSourcesRule(
      RequestContext requestContext, CreateMaliciousSourcesRuleRequest createRuleRequest);

  MaliciousSourcesRule updateMaliciousSourcesRule(
      RequestContext requestContext, UpdateMaliciousSourcesRuleRequest updateRuleRequest);

  MaliciousSourcesRule deleteMaliciousSourcesRule(RequestContext requestContext, String id);
}
