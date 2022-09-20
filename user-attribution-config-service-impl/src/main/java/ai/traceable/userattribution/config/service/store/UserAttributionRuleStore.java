package ai.traceable.userattribution.config.service.store;

import static ai.traceable.userattribution.config.service.store.UserAttributionRuleScopeUtils.setUserAttributionRuleScopeIfNotPresent;

import ai.traceable.config.utils.RankCalculator;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter;
import ai.traceable.userattribution.config.service.v1.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter.ScopeFilter;
import ai.traceable.userattribution.config.service.v1.UserAttributionRule;
import ai.traceable.userattribution.config.service.v1.UserAttributionRuleScope.EnvironmentScope;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class UserAttributionRuleStore
    extends IdentifiedObjectStoreWithFilter<UserAttributionRule, GetUserAttributionRulesFilter> {
  private static final String USER_ATTRIBUTION_RULE_RESOURCE_NAME = "user-attribution-rule";
  private static final String USER_ATTRIBUTION_RESOURCE_NAMESPACE = "user-attribution";

  private final RankCalculator<UserAttributionRule, String> rankCalculator;

  @Inject
  public UserAttributionRuleStore(
      ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      RankCalculator<UserAttributionRule, String> rankCalculator) {
    super(
        configServiceBlockingStub,
        USER_ATTRIBUTION_RESOURCE_NAMESPACE,
        USER_ATTRIBUTION_RULE_RESOURCE_NAME,
        configChangeEventGenerator);
    this.rankCalculator = rankCalculator;
  }

  public List<UserAttributionRule> getAllData(RequestContext requestContext) {
    return this.getAllObjects(requestContext).stream()
        .map(ContextualConfigObject::getData)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  protected Optional<UserAttributionRule> buildDataFromValue(Value value) {
    UserAttributionRule.Builder builder = UserAttributionRule.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      setUserAttributionRuleScopeIfNotPresent(builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Failed to convert config to UserAttributionRule: {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(UserAttributionRule data) {
    UserAttributionRule.Builder builder = data.toBuilder();
    setUserAttributionRuleScopeIfNotPresent(builder);
    return ConfigProtoConverter.convertToValue(builder);
  }

  @Override
  protected String getContextFromData(UserAttributionRule data) {
    return data.getId();
  }

  @Override
  protected List<ContextualConfigObject<UserAttributionRule>> orderFetchedObjects(
      List<ContextualConfigObject<UserAttributionRule>> objects) {
    return this.rankCalculator.orderFromRanks(objects, ContextualConfigObject::getData);
  }

  @Override
  protected Optional<UserAttributionRule> filterConfigData(
      UserAttributionRule data, GetUserAttributionRulesFilter filter) {
    return Optional.of(data)
        .filter(rule -> !filter.hasDisabled() || rule.getDisabled() == filter.getDisabled())
        .filter(rule -> filterRuleOnScope(data, filter.getScopeFilter()));
  }

  private boolean filterRuleOnScope(UserAttributionRule ruleData, ScopeFilter scopeFilter) {
    List<EnvironmentScope> ruleEnvironments =
        ruleData.getScope().getCustomScope().getEnvironmentScopesList();
    // when environment scope is requested in the filter, with no environments assigned, return only
    // those rules which are not targeted at an environment.
    if (!scopeFilter.hasEnvironmentScopeFilter() || ruleEnvironments.isEmpty()) {
      return true;
    }
    return scopeFilter.getEnvironmentScopeFilter().getEnvironmentNamesList().stream()
        .map(env -> EnvironmentScope.newBuilder().setEnvironmentName(env).build())
        .anyMatch(ruleEnvironments::contains);
  }
}
