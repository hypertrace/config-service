package ai.traceable.external.data.classification.config.service.legacy;

import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import com.google.inject.Inject;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = @Inject)
class RedactionRulesDao {
  private final SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub;

  List<RedactionRule> getRulesMatchFilter(
      RequestContext requestContext, RedactionRuleFilter filter) {
    return requestContext.call(
        () ->
            sensitiveDataConfigServiceBlockingStub
                .getAllRedactionRules(GetAllRedactionRulesRequest.getDefaultInstance())
                .getRedactionRulesList()
                .stream()
                .filter(rule -> filter.getEnabledRuleIds().contains(rule.getId()))
                .collect(Collectors.toUnmodifiableList()));
  }

  RedactionStrategy getParamTypeHeaderRedactionStrategy(RequestContext requestContext) {
    return requestContext.call(
        () ->
            sensitiveDataConfigServiceBlockingStub
                .getRedactionStrategyForType(
                    GetRedactionStrategyForTypeRequest.newBuilder()
                        .setParamType(ParamType.PARAM_TYPE_HEADER)
                        .build())
                .getRedactionStrategy());
  }

  @Value
  static class RedactionRuleFilter {
    Set<String> enabledRuleIds;
  }
}
