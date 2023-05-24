package ai.traceable.sessionattribution.config.service.store;

import ai.traceable.config.utils.RankCalculator;
import ai.traceable.sessionattribution.config.service.v1.GetSessionAttributionRulesRequest;
import ai.traceable.sessionattribution.config.service.v1.SessionAttributionRule;
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
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class SessionAttributionRuleStore
    extends IdentifiedObjectStoreWithFilter<
        SessionAttributionRule,
        GetSessionAttributionRulesRequest.GetSessionAttributionRulesFilter> {
  private static final String SESSION_ATTRIBUTION_RULE_RESOURCE_NAME = "session-attribution-rule";
  private static final String SESSION_ATTRIBUTION_RESOURCE_NAMESPACE = "session-attribution";

  private final RankCalculator<SessionAttributionRule, String> rankCalculator;

  @Inject
  public SessionAttributionRuleStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator,
      RankCalculator<SessionAttributionRule, String> rankCalculator) {
    super(
        configServiceBlockingStub,
        SESSION_ATTRIBUTION_RESOURCE_NAMESPACE,
        SESSION_ATTRIBUTION_RULE_RESOURCE_NAME,
        configChangeEventGenerator);
    this.rankCalculator = rankCalculator;
  }

  public List<SessionAttributionRule> getAllData(RequestContext requestContext) {
    return this.getAllObjects(requestContext).stream()
        .map(ContextualConfigObject::getData)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  protected Optional<SessionAttributionRule> buildDataFromValue(Value value) {
    SessionAttributionRule.Builder builder = SessionAttributionRule.newBuilder();
    try {
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (InvalidProtocolBufferException e) {
      log.error("Failed to convert config to SessionAttributionRule: {}", value, e);
      return Optional.empty();
    }
  }

  @SneakyThrows
  @Override
  protected Value buildValueFromData(SessionAttributionRule data) {
    SessionAttributionRule.Builder builder = data.toBuilder();
    return ConfigProtoConverter.convertToValue(builder);
  }

  @Override
  protected String getContextFromData(SessionAttributionRule data) {
    return data.getId();
  }

  @Override
  protected List<ContextualConfigObject<SessionAttributionRule>> orderFetchedObjects(
      List<ContextualConfigObject<SessionAttributionRule>> objects) {
    return this.rankCalculator.orderFromRanks(objects, ContextualConfigObject::getData);
  }

  @Override
  protected Optional<SessionAttributionRule> filterConfigData(
      SessionAttributionRule data,
      GetSessionAttributionRulesRequest.GetSessionAttributionRulesFilter filter) {
    return Optional.of(data)
        .filter(
            rule -> !filter.hasDisabled() || rule.getStatus().getDisabled() == filter.getDisabled())
        .filter(rule -> filterRuleOnScope(data, filter));
  }

  /**
   * Method to filter on rule-scope * If filterScope has no environment scope, always return true *
   * If filterScope has environment scope but the environment scope has no environment IDs, return
   * true only if the rule has no Environment IDs in its rule-scope. * If filterScope has
   * environment scope and the environment scope has one or more environment IDs, return true only
   * if there is at least one overlap of environment ID between the filter and the rule.
   */
  private boolean filterRuleOnScope(
      SessionAttributionRule rule,
      GetSessionAttributionRulesRequest.GetSessionAttributionRulesFilter filter) {
    List<String> ruleEnvironmentNamesList = rule.getScope().getEnvironmentNamesList();
    if (!filter.hasEnvironmentFilter() || ruleEnvironmentNamesList.isEmpty()) {
      return true;
    }

    List<String> filterEnvironmentNamesList =
        filter.getEnvironmentFilter().getEnvironmentNamesList();
    return ruleEnvironmentNamesList.stream().anyMatch(filterEnvironmentNamesList::contains);
  }
}
