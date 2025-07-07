package ai.traceable.ratelimiting.service.v2.rules.migration;

import ai.traceable.ratelimiting.config.service.v2.Action;
import ai.traceable.ratelimiting.config.service.v2.CreateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RuleEvaluationPoint;
import ai.traceable.ratelimiting.config.service.v2.ThresholdActionConfig;
import ai.traceable.ratelimiting.config.service.v2.TransactionActionConfig;
import ai.traceable.ratelimiting.config.service.v2.UpdateRateLimitingRuleRequest;
import ai.traceable.ratelimiting.service.v2.rules.modsec.ModsecRuleSupportChecker;
import ai.traceable.ratelimiting.service.v2.rules.shared.RateLimitingRulesEdgeDecisionFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class RateLimitingRuleEvaluationPointsMigrator {
  public CreateRateLimitingRuleRequest migrateCreateRateLimitingRuleRequest(
      CreateRateLimitingRuleRequest createRateLimitingRuleRequest) {
    return migrateRequest(
        createRateLimitingRuleRequest,
        CreateRateLimitingRuleRequest::hasData,
        CreateRateLimitingRuleRequest::getData,
        (request, data) -> request.toBuilder().setData(data).build());
  }

  public UpdateRateLimitingRuleRequest migrateUpdateRateLimitingRuleRequest(
      UpdateRateLimitingRuleRequest updateRateLimitingRuleRequest) {
    return migrateRequest(
        updateRateLimitingRuleRequest,
        UpdateRateLimitingRuleRequest::hasData,
        UpdateRateLimitingRuleRequest::getData,
        (request, data) -> request.toBuilder().setData(data).build());
  }

  private <T> T migrateRequest(
      T request,
      Predicate<T> hasDataPredicate,
      Function<T, RateLimitingRuleData> dataExtractor,
      BiFunction<T, RateLimitingRuleData, T> requestUpdater) {

    return Optional.of(request)
        .filter(hasDataPredicate)
        .map(dataExtractor)
        .filter(data -> data.getRuleEvaluationPointsList().isEmpty())
        .map(
            data ->
                data.toBuilder().addAllRuleEvaluationPoints(getRuleEvaluationPoints(data)).build())
        .map(updatedData -> requestUpdater.apply(request, updatedData))
        .orElse(request);
  }

  public List<RuleEvaluationPoint> getRuleEvaluationPoints(RateLimitingRuleData ruleData) {
    List<RuleEvaluationPoint> ruleEvaluationPoints = new ArrayList<>();

    // check for platform rule evaluation point
    if (!containsBlockingForDurationBasedActionConfig(ruleData)) {
      ruleEvaluationPoints.add(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM);
    }

    // check for edge
    if (RateLimitingRulesEdgeDecisionFilter.meetsEdgeDecisionRequirements(ruleData)) {
      ruleEvaluationPoints.add(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE);
    }

    // check for agent
    if (ruleData.hasTransactionActionConfig()) {
      if (ModsecRuleSupportChecker.meetsInlineTracingAgentActionRequirements(
          ruleData.getTransactionActionConfig())) {
        ruleEvaluationPoints.add(RuleEvaluationPoint.RULE_EVALUATION_POINT_INLINE_TRACING_AGENT);
      }
    }
    return ruleEvaluationPoints;
  }

  private boolean containsBlockingForDurationBasedActionConfig(RateLimitingRuleData ruleData) {
    return containsBlockingForDurationBasedThresholdActionConfig(
            ruleData.getThresholdActionConfigsList())
        || containsBlockingForDurationBasedTransactionActionConfig(
            ruleData.getTransactionActionConfig());
  }

  private boolean containsBlockingForDurationBasedThresholdActionConfig(
      List<ThresholdActionConfig> thresholdActionConfigs) {
    return thresholdActionConfigs.stream()
        .flatMap(thresholdActionConfig -> thresholdActionConfig.getActionsList().stream())
        .filter(Action::hasBlock)
        .map(Action::getBlock)
        .anyMatch(Action.Block::getUseThresholdDuration);
  }

  private boolean containsBlockingForDurationBasedTransactionActionConfig(
      TransactionActionConfig transactionActionConfig) {
    return transactionActionConfig.hasAction()
        && transactionActionConfig.getAction().hasBlock()
        && transactionActionConfig.getAction().getBlock().getUseThresholdDuration();
  }
}
