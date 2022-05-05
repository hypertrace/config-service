package ai.traceable.ratelimiting.service.v2.rules;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.ratelimiting.config.service.v2.Category;
import ai.traceable.ratelimiting.config.service.v2.GetRulesByCategoryFilter;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingRulesManager implements RulesManager {
  private final RateLimitingRulesStore rateLimitingRulesStore;
  private final UuidGenerator uuidGenerator;

  @Inject
  public RateLimitingRulesManager(
      RateLimitingRulesStore rateLimitingRulesStore, UuidGenerator uuidGenerator) {
    this.rateLimitingRulesStore = rateLimitingRulesStore;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public List<RateLimitingRule> getRateLimitingRules(
      RequestContext requestContext, GetRulesByCategoryFilter filter) {
    List<Category> categories = filter.getCategoriesList();
    List<RateLimitingRule> rateLimitingRules =
        rateLimitingRulesStore.getAllObjects(requestContext).stream()
            .map(ConfigObject::getData)
            .collect(Collectors.toList());
    return categories.isEmpty()
        ? rateLimitingRules
        : rateLimitingRules.stream()
            .filter(rule -> categories.contains(rule.getData().getCategory()))
            .collect(Collectors.toList());
  }

  @Override
  public RateLimitingRule updateRateLimitingRule(
      RequestContext requestContext, String ruleId, RateLimitingRuleData ruleData) {
    RateLimitingRule rule =
        rateLimitingRulesStore
            .getData(requestContext, ruleId)
            .orElseThrow(Status.NOT_FOUND::asRuntimeException);
    RateLimitingRule modifiedRule = rule.toBuilder().setData(ruleData).build();
    return rateLimitingRulesStore.upsertObject(requestContext, modifiedRule).getData();
  }

  @Override
  public RateLimitingRule createRateLimitingRule(
      RequestContext requestContext, RateLimitingRuleData ruleData) {
    RateLimitingRule rule =
        RateLimitingRule.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setData(ruleData)
            .build();
    return rateLimitingRulesStore.upsertObject(requestContext, rule).getData();
  }

  @Override
  public RateLimitingRule deleteRateLimitingRule(RequestContext requestContext, String ruleId) {
    return rateLimitingRulesStore
        .deleteObject(requestContext, ruleId)
        .map(ConfigObject::getData)
        .orElseThrow(Status.NOT_FOUND::asRuntimeException);
  }
}
