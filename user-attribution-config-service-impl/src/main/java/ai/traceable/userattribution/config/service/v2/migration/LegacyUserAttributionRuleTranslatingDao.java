package ai.traceable.userattribution.config.service.v2.migration;

import ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface LegacyUserAttributionRuleTranslatingDao {
  List<UserAttributionRule> getAllUserAttributionRulesFromLegacyStore(
      RequestContext requestContext);

  List<UserAttributionRule> getUserAttributionRulesFromLegacyStore(
      RequestContext requestContext, GetUserAttributionRulesFilter filter);

  void deleteMultipleUserAttributionRulesFromLegacyStore(
      RequestContext requestContext, List<String> ruleIds);
}
