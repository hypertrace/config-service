package ai.traceable.ratelimiting.service.v2.rules;

import static ai.traceable.platform.utils.ip.IpAddressParsingUtils.parseRawIpRange;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.platform.utils.ip.IpAddressParsingUtils.IpParsingResults;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.IpAddressCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceConfig;
import com.google.inject.Inject;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.DeletedConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingRulesManager implements RulesManager {
  private final RateLimitingRulesStore rateLimitingRulesStore;
  private final UuidGenerator uuidGenerator;
  private final RateLimitingConfigServiceConfig rateLimitingConfigServiceConfig;

  @Inject
  public RateLimitingRulesManager(
      RateLimitingRulesStore rateLimitingRulesStore,
      UuidGenerator uuidGenerator,
      RateLimitingConfigServiceConfig rateLimitingConfigServiceConfig) {
    this.rateLimitingRulesStore = rateLimitingRulesStore;
    this.uuidGenerator = uuidGenerator;
    this.rateLimitingConfigServiceConfig = rateLimitingConfigServiceConfig;
  }

  @Override
  public List<RateLimitingRule> getRateLimitingRules(
      RequestContext requestContext, GetRateLimitingRulesFilter filter) {
    if (filter.equals(GetRateLimitingRulesFilter.getDefaultInstance())) {
      return rateLimitingRulesStore.getAllConfigData(requestContext);
    }
    return rateLimitingRulesStore.getAllConfigData(requestContext, filter);
  }

  @Override
  public RateLimitingRule updateRateLimitingRule(
      RequestContext requestContext, String ruleId, RateLimitingRuleData ruleData) {
    RateLimitingRule rule =
        rateLimitingRulesStore
            .getData(requestContext, ruleId)
            .orElseGet(
                () ->
                    rateLimitingConfigServiceConfig.getDefaultRateLimitingRules().stream()
                        .filter(rateLimitingRule -> rateLimitingRule.getId().equals(ruleId))
                        .findFirst()
                        .orElseThrow(Status.NOT_FOUND::asRuntimeException));

    RateLimitingRule modifiedRule =
        rule.toBuilder().setData(processRateLimitRuleData(ruleData)).build();
    return rateLimitingRulesStore.upsertObject(requestContext, modifiedRule).getData();
  }

  @Override
  public RateLimitingRule createRateLimitingRule(
      RequestContext requestContext, RateLimitingRuleData ruleData) {
    RateLimitingRule rule =
        RateLimitingRule.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setData(processRateLimitRuleData(ruleData))
            .build();
    return rateLimitingRulesStore.upsertObject(requestContext, rule).getData();
  }

  public RateLimitingRuleData processRateLimitRuleData(RateLimitingRuleData data) {
    if (data.hasCondition()) {
      RateLimitingRuleData.Builder builder = data.toBuilder();
      builder.setCondition(processCondition(data.getCondition()));
      return builder.build();
    }
    return data;
  }

  private Condition processCondition(Condition condition) {
    Condition.Builder builder = condition.toBuilder();
    if (condition.hasLeafCondition() && condition.getLeafCondition().hasIpAddressCondition()) {
      List<String> rawIps =
          condition.getLeafCondition().getIpAddressCondition().getRawInputIpDataList();
      IpParsingResults parsedResults = parseRawIpRange(rawIps);
      IpAddressCondition.Builder ipAddressConditionBuilder =
          builder.getLeafConditionBuilder().getIpAddressConditionBuilder();
      ipAddressConditionBuilder.clearCidrIpRanges().clearIpAddresses();
      ipAddressConditionBuilder
          .addAllCidrIpRanges(parsedResults.getIpRanges())
          .addAllIpAddresses(parsedResults.getIpAddresses());
    } else if (condition.hasCompositeCondition()) {
      List<Condition> processedChildrenConditions =
          condition.getCompositeCondition().getChildrenList().stream()
              .map(this::processCondition)
              .collect(Collectors.toList());
      builder.getCompositeConditionBuilder().clearChildren();
      builder.getCompositeConditionBuilder().addAllChildren(processedChildrenConditions);
    }
    return builder.build();
  }

  @Override
  public Optional<RateLimitingRule> deleteRateLimitingRule(
      RequestContext requestContext, String ruleId) {
    return rateLimitingRulesStore
        .deleteObject(requestContext, ruleId)
        .map(DeletedConfigObject::getDeletedData)
        .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
  }
}
