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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = @Inject)
class RedactionRulesDao {
  private final SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub;
  private final ClientConfig clientConfig;

  List<RedactionRule> getRulesMatchFilter(
      RequestContext requestContext, RedactionRuleFilter filter) {
    return requestContext.call(
        () ->
            sensitiveDataConfigServiceBlockingStub
                .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .getAllRedactionRules(GetAllRedactionRulesRequest.getDefaultInstance())
                .getRedactionRulesList()
                .stream()
                .filter(rule -> this.checkFilter(rule, filter))
                .collect(Collectors.toUnmodifiableList()));
  }

  RedactionStrategy getParamTypeHeaderRedactionStrategy(RequestContext requestContext) {
    return requestContext.call(
        () ->
            sensitiveDataConfigServiceBlockingStub
                .withDeadlineAfter(clientConfig.getTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .getRedactionStrategyForType(
                    GetRedactionStrategyForTypeRequest.newBuilder()
                        .setParamType(ParamType.PARAM_TYPE_HEADER)
                        .build())
                .getRedactionStrategy());
  }

  private boolean checkFilter(RedactionRule rule, RedactionRuleFilter filter) {
    if (rule.getDisabled()) {
      return false; // If the source rule is disabled, we should never use it
    }
    // Otherwise, we grab the session rules and those that have a corresponding enabled data type
    if (filter.isAlwaysIncludeSessionIdentifier() && rule.getSessionIdentifier()) {
      return true;
    }
    return filter.getEnabledLegacyDataTypeIds().contains(rule.getId());
  }

  @Value
  static class RedactionRuleFilter {
    Set<String> enabledLegacyDataTypeIds;
    boolean alwaysIncludeSessionIdentifier = true;
  }
}
