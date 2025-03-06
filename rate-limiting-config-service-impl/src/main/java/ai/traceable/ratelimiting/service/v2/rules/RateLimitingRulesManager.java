package ai.traceable.ratelimiting.service.v2.rules;

import static ai.traceable.platform.utils.ip.IpAddressParsingUtils.parseRawIpRange;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.platform.utils.ip.IpAddressParsingUtils.IpParsingResults;
import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.Condition;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingModsecRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRuleModsecRulesResponse;
import ai.traceable.ratelimiting.config.service.v2.GetRateLimitingRulesFilter;
import ai.traceable.ratelimiting.config.service.v2.IpAddressCondition;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RuleStatus;
import ai.traceable.ratelimiting.config.service.v2.TransactionActionConfig;
import ai.traceable.ratelimiting.service.v2.RateLimitingConfigServiceConfig;
import ai.traceable.ratelimiting.service.v2.rules.modsec.RateLimitingModsecRulesManager;
import com.google.inject.Inject;
import io.grpc.Status;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.DeletedConfigObject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class RateLimitingRulesManager implements RulesManager {
  private final RateLimitingRulesStore rateLimitingRulesStore;
  private final UuidGenerator uuidGenerator;
  private final RateLimitingConfigServiceConfig rateLimitingConfigServiceConfig;
  private final RateLimitingModsecRulesManager modsecRulesManager;
  private final Clock clock;

  @Inject
  public RateLimitingRulesManager(
      RateLimitingRulesStore rateLimitingRulesStore,
      UuidGenerator uuidGenerator,
      RateLimitingConfigServiceConfig rateLimitingConfigServiceConfig,
      RateLimitingModsecRulesManager modsecRulesManager,
      Clock clock) {
    this.rateLimitingRulesStore = rateLimitingRulesStore;
    this.uuidGenerator = uuidGenerator;
    this.rateLimitingConfigServiceConfig = rateLimitingConfigServiceConfig;
    this.modsecRulesManager = modsecRulesManager;
    this.clock = clock;
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
      RequestContext requestContext, String ruleId, RateLimitingRuleData newRuleData) {
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
        rule.toBuilder().setData(mergeRateLimitRuleData(newRuleData, rule.getData())).build();
    return rateLimitingRulesStore.upsertObject(requestContext, modifiedRule).getData();
  }

  private RateLimitingRuleData mergeRateLimitRuleData(
      RateLimitingRuleData newRuleData, RateLimitingRuleData oldRuleData) {
    if (!newRuleData.hasRuleStatus()) {
      return oldRuleData;
    }
    RateLimitingRuleData transformedRateLimitRuleData =
        applyRateLimitRuleDataTransformations(newRuleData);
    RuleStatus newRuleStatus = transformedRateLimitRuleData.getRuleStatus();
    RuleStatus oldRuleStatus = oldRuleData.getRuleStatus();
    RuleStatus mergedRuleStatus =
        oldRuleStatus.toBuilder()
            .mergeFrom(newRuleStatus)
            .setInternal(newRuleStatus.getInternal())
            .setRuleCreationSource(oldRuleStatus.getRuleCreationSource())
            .build();
    return transformedRateLimitRuleData.toBuilder().setRuleStatus(mergedRuleStatus).build();
  }

  @Override
  public RateLimitingRule createRateLimitingRule(
      RequestContext requestContext, RateLimitingRuleData ruleData) {
    RateLimitingRule rule =
        RateLimitingRule.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setData(applyRateLimitRuleDataTransformations(ruleData))
            .build();
    return rateLimitingRulesStore.upsertObject(requestContext, rule).getData();
  }

  public RateLimitingRuleData applyRateLimitRuleDataTransformations(RateLimitingRuleData data) {
    RateLimitingRuleData.Builder builder = data.toBuilder();
    if (data.hasCondition()) {
      builder.setCondition(processCondition(data.getCondition()));
    }
    if (data.hasTransactionActionConfig()) {
      builder.setTransactionActionConfig(
          processTransactionActionConfig(data.getTransactionActionConfig()));
    }
    return builder.build();
  }

  public TransactionActionConfig processTransactionActionConfig(
      TransactionActionConfig transactionActionConfig) {
    if (transactionActionConfig.hasExpirationTimestampMillis()) {
      return transactionActionConfig;
    }
    TransactionActionConfig.Builder builder = transactionActionConfig.toBuilder();
    Action action = transactionActionConfig.getAction();
    String durationIso = null;
    if (action.hasAllow() && action.getAllow().hasDurationIso()) {
      durationIso = transactionActionConfig.getAction().getAllow().getDurationIso();
    } else if (action.hasBlock() && action.getBlock().hasDurationIso()) {
      durationIso = transactionActionConfig.getAction().getBlock().getDurationIso();
    }
    if (durationIso != null) {
      builder.setExpirationTimestampMillis(clock.millis() + Duration.parse(durationIso).toMillis());
    }
    return builder.build();
  }

  private Condition processCondition(Condition condition) {
    Condition.Builder builder = condition.toBuilder();
    if (condition.hasLeafCondition() && condition.getLeafCondition().hasIpAddressCondition()) {
      IpAddressCondition ipAddressCondition = condition.getLeafCondition().getIpAddressCondition();
      List<String> rawIps = ipAddressCondition.getRawInputIpDataList();
      if (!rawIps.isEmpty()) {
        IpParsingResults parsedResults = parseRawIpRange(rawIps);
        IpAddressCondition.Builder ipAddressConditionBuilder =
            builder.getLeafConditionBuilder().getIpAddressConditionBuilder();
        ipAddressConditionBuilder
            .addAllCidrIpRanges(parsedResults.getIpRanges())
            .addAllIpAddresses(parsedResults.getIpAddresses())
            .setExclude(ipAddressCondition.getExclude());
      }
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
    Optional<RateLimitingRule> deletedRule =
        rateLimitingRulesStore
            .deleteObject(requestContext, ruleId)
            .flatMap(DeletedConfigObject::getDeletedData);

    if (deletedRule.isEmpty() && !isDefaultRule(requestContext, ruleId)) {
      throw Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers());
    }

    return deletedRule;
  }

  private boolean isDefaultRule(RequestContext requestContext, String ruleId) {
    return rateLimitingRulesStore
        .getData(requestContext, ruleId)
        .map(
            rule ->
                rule.getData()
                    .getRuleStatus()
                    .getRuleCreationSource()
                    .equals(RuleStatus.RuleSource.RULE_SOURCE_DEFAULT))
        .orElse(false);
  }

  @Override
  public GetRateLimitingRuleModsecRulesResponse getRateLimitingModsecRules(
      RequestContext requestContext, GetRateLimitingModsecRulesFilter filter) {
    return modsecRulesManager.getRateLimitingModsecRules(
        requestContext, filter, filter2 -> getRateLimitingRules(requestContext, filter2));
  }
}
