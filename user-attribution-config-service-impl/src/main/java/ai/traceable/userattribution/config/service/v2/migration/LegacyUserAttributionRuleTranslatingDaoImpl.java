package ai.traceable.userattribution.config.service.v2.migration;

import ai.traceable.userattribution.config.service.v1.store.UserAttributionRuleStore;
import ai.traceable.userattribution.config.service.v2.GetUserAttributionRulesRequest;
import ai.traceable.userattribution.config.service.v2.UserAttributionRule;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
public class LegacyUserAttributionRuleTranslatingDaoImpl
    implements LegacyUserAttributionRuleTranslatingDao {
  private final UserAttributionRuleStore legacyRuleStore;
  private final UserAttributionRuleConverter ruleConverter;
  private final UserAttributionFilterConverter filterConverter;

  @Inject
  public LegacyUserAttributionRuleTranslatingDaoImpl(
      UserAttributionRuleStore legacyRuleStore,
      UserAttributionRuleConverter ruleConverter,
      UserAttributionFilterConverter filterConverter) {
    this.legacyRuleStore = legacyRuleStore;
    this.ruleConverter = ruleConverter;
    this.filterConverter = filterConverter;
  }

  @Override
  public List<UserAttributionRule> getAllUserAttributionRulesFromLegacyStore(
      RequestContext requestContext) {
    return legacyRuleStore.getAllConfigData(requestContext).stream()
        .map(ruleConverter::convert)
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<UserAttributionRule> getUserAttributionRulesFromLegacyStore(
      RequestContext requestContext,
      GetUserAttributionRulesRequest.GetUserAttributionRulesFilter filter) {
    return legacyRuleStore
        .getAllConfigData(requestContext, filterConverter.convert(filter))
        .stream()
        .map(ruleConverter::convert)
        .flatMap(Optional::stream)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public void deleteMultipleUserAttributionRulesFromLegacyStore(
      RequestContext requestContext, List<String> ruleIds) {
    legacyRuleStore.deleteObjects(requestContext, ruleIds);
  }
}
