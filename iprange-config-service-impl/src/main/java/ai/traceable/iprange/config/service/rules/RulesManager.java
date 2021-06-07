package ai.traceable.iprange.config.service.rules;

import ai.traceable.iprange.config.service.v1.CreateIpRangeRuleRequest;
import ai.traceable.iprange.config.service.v1.GetRulesFilter;
import ai.traceable.iprange.config.service.v1.IpRangeRule;
import ai.traceable.iprange.config.service.v1.UpdateIpRangeRuleRequest;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface RulesManager {

  List<IpRangeRule> getIpRangeRules(RequestContext requestContext, GetRulesFilter filter);

  IpRangeRule createIpRangeRule(
      RequestContext requestContext, CreateIpRangeRuleRequest createRuleRequest);

  IpRangeRule updateIpRangeRule(
      RequestContext requestContext, UpdateIpRangeRuleRequest updateIpRangeRuleRequest);

  void deleteIpRangeRule(RequestContext requestContext, String id);
}
