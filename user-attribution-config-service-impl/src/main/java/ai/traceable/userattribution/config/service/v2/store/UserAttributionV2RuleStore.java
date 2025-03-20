package ai.traceable.userattribution.config.service.v2.store;

import static ai.traceable.userattribution.config.service.v2.Source.SOURCE_UNSPECIFIED;
import static ai.traceable.userattribution.config.service.v2.Source.SOURCE_USER;
import static java.util.stream.Collectors.toUnmodifiableList;

import ai.traceable.config.utils.RankCalculator;
import ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter;
import ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest.GetUserAttributionRulesFilter.EnvironmentFilter;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleScope;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Value;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.config.objectstore.ContextualConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;

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
  public List<UserAttributionRule> getAllConfigData(
      RequestContext requestContext, GetUserAttributionRulesFilter filter) {
    return super.getAllConfigData(requestContext, filter).stream()
        .map(this::setSourceIfNotSet)
        .collect(toUnmodifiableList());
  }

  @Override
  public List<UserAttributionRule> getAllConfigData(RequestContext requestContext) {
    return super.getAllConfigData(requestContext).stream()
        .map(this::setSourceIfNotSet)
        .collect(toUnmodifiableList());
  }

  @Override
  public Optional<UserAttributionRule> getData(RequestContext context, String id) {
    return super.getData(context, id).map(this::setSourceIfNotSet);
  }

  @Override
  public ContextualConfigObject<UserAttributionRule> upsertObject(
      RequestContext context, UserAttributionRule rule) {
    UserAttributionRule ruleWithSource = setSourceIfNotSet(rule);
    return super.upsertObject(context, ruleWithSource);
  }

  @Override
  public List<ContextualConfigObject<UserAttributionRule>> upsertObjects(
      RequestContext context, List<UserAttributionRule> rules) {
    List<UserAttributionRule> rulesWithSource =
        rules.stream().map(this::setSourceIfNotSet).collect(toUnmodifiableList());
    return super.upsertObjects(context, rulesWithSource);
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

  private UserAttributionRule setSourceIfNotSet(UserAttributionRule rule) {
    // For backward compatibility
    if (SOURCE_UNSPECIFIED.equals(rule.getData().getSource())) {
      return rule.toBuilder().setData(rule.getData().toBuilder().setSource(SOURCE_USER)).build();
    }
    return rule;
  }
}
