package ai.traceable.sensitivedata.config.service;

import ai.traceable.sensitivedata.config.service.v1.FullPrivacyModeConfig;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.NewRedactionRule;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;

interface ConfigServiceCoordinator {

  void upsertParamTypeRedactionStrategyConfig(
      RequestContext requestContext,
      ParamType paramType,
      ParamTypeRedactionStrategyConfig paramTypeRedactionStrategyConfig);

  RedactionStrategy getParamTypeRedactionStrategy(
      RequestContext requestContext, ParamType paramType);

  void upsertAutomaticSecretRedactionStrategyConfig(
      RequestContext requestContext,
      AutomaticSecretRedactionStrategyConfig automaticSecretRedactionStrategyConfig);

  boolean isAutomaticSecretRedactionStrategyEnabled(RequestContext requestContext);

  RedactionRule createRedactionRule(
      RequestContext requestContext, NewRedactionRule newRedactionRule);

  RedactionRule updateRedactionRule(RequestContext requestContext, RedactionRule redactionRule);

  List<RedactionRule> getRedactionRules(RequestContext requestContext, boolean includeConditional);

  List<RedactionRule> getAllRedactionRules(
      RequestContext requestContext, GetAllRedactionRulesRequest.RedactionRuleFilter filter);

  RedactionRule deleteRedactionRule(RequestContext requestContext, String redactionRuleId);

  boolean isFullPrivacyModeEnabled(RequestContext requestContext);

  void upsertFullPrivacyModeConfig(
      RequestContext requestContext, FullPrivacyModeConfig fullPrivacyModeConfig);
}
