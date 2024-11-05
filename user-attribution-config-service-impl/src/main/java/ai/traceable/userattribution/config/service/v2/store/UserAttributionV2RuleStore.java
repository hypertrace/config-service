package ai.traceable.userattribution.config.service.v2.store;

import ai.traceable.config.utils.RankCalculator;
import ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter;
import ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter.EnvironmentFilter;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleScope;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;

@Slf4j
public class UserAttributionV2RuleStore
    extends IdentifiedObjectStoreWithFilter<UserAttributionRule, GetUserAttributionRulesFilter> {
  private static final String USER_ATTRIBUTION_RULE_RESOURCE_NAME = "user-attribution-rule-v2";
  private static final String USER_ATTRIBUTION_RESOURCE_NAMESPACE = "user-attribution";

  private final RankCalculator<UserAttributionRule, String> rankCalculator;

  @Inject
  public UserAttributionV2RuleStore(
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

  @Override
  protected Optional<UserAttributionRule> buildDataFromValue(Value value) {
    UserAttributionRule.Builder builder = UserAttributionRule.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
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
        .filter(
            rule -> !filter.hasDisabled() || rule.getData().getDisabled() == filter.getDisabled())
        .filter(
            rule ->
                !filter.hasEnvironmentFilter()
                    || filterRuleOnScope(data, filter.getEnvironmentFilter()));
  }

  private boolean filterRuleOnScope(UserAttributionRule data, EnvironmentFilter environmentFilter) {
    UserAttributionRuleScope scope = data.getData().getScope();
    // if rule doesn't have environment scope, then it matches irrespective of environment names in
    // filter
    if (!scope.hasEnvironmentScope()
        || scope.getEnvironmentScope().getEnvironmentNames().getValuesList().isEmpty()) {
      return true;
    }
    Set<String> environmentNamesInFilter = Set.copyOf(environmentFilter.getEnvironmentNamesList());
    Set<String> ruleEnvironmentNames =
        Set.copyOf(scope.getEnvironmentScope().getEnvironmentNames().getValuesList());
    return !Sets.intersection(environmentNamesInFilter, ruleEnvironmentNames).isEmpty();
  }
}
