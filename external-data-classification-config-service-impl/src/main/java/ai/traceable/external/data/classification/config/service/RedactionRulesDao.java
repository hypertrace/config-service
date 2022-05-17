package ai.traceable.external.data.classification.config.service;

import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import com.google.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;

class RedactionRulesDao {
  private final SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub;

  @Inject
  public RedactionRulesDao(
      SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub) {
    this.sensitiveDataConfigServiceBlockingStub = sensitiveDataConfigServiceBlockingStub;
  }

  public List<RedactionRule> getAllRedactionRules(RequestContext requestContext) {
    return requestContext.call(
        () ->
            sensitiveDataConfigServiceBlockingStub
                .getAllRedactionRules(GetAllRedactionRulesRequest.getDefaultInstance())
                .getRedactionRulesList()
                .stream()
                .collect(Collectors.toUnmodifiableList()));
  }

  public RedactionStrategy getParamTypeHeaderRedactionStrategy(RequestContext requestContext) {
    return requestContext.call(
        () ->
            sensitiveDataConfigServiceBlockingStub
                .getRedactionStrategyForType(
                    GetRedactionStrategyForTypeRequest.newBuilder()
                        .setParamType(ParamType.PARAM_TYPE_HEADER)
                        .build())
                .getRedactionStrategy());
  }
}
