package ai.traceable.sensitivedata.config.service;

import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import org.hypertrace.core.grpcutils.context.RequestContext;

public interface ConfigServiceCoordinator {

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
}
