package ai.traceable.ratelimiting.service.v2.rules.converter;

import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT;

import ai.traceable.edge.decision.config.service.v1.EdgeDecision;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleStatus;
import ai.traceable.edge.decision.config.service.v1.EnvironmentScope;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRule;
import ai.traceable.ratelimiting.config.service.v2.RateLimitingRuleData;
import ai.traceable.ratelimiting.config.service.v2.RuleConfigScope;
import ai.traceable.ratelimiting.config.service.v2.RuleStatus;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class RateLimitingEdgeDecisionConverter {

  public EdgeDecisionEngineConfig convert(List<RateLimitingRule> rateLimitingRules) {
    EdgeDecisionEngineConfig.Builder builder = EdgeDecisionEngineConfig.newBuilder();
    List<EdgeDecisionRule> edgeDecisionRules =
        rateLimitingRules.stream()
            .map(this::convertRateLimitingRule)
            .collect(Collectors.toUnmodifiableList());
    builder.addAllDecisionRules(edgeDecisionRules);
    return builder.build();
  }

  private EdgeDecisionRule convertRateLimitingRule(RateLimitingRule rateLimitingRule) {
    EdgeDecisionRule.Builder builder = EdgeDecisionRule.newBuilder();
    builder.setId(rateLimitingRule.getId());
    RateLimitingRuleData data = rateLimitingRule.getData();
    builder.setName(data.getName());
    builder.setRuleStatus(buildRuleStatus(data));
    builder.setRuleCategory(EDGE_DECISION_RULE_CATEGORY_RATE_LIMIT);
    buildRuleScope(data).ifPresent(builder::setRuleScope);
    builder.setRuleDecision(buildRuleDecision(data));
    builder.setRuleDefinition(buildRuleDefinition(data));
    return builder.build();
  }

  private EdgeDecisionRuleStatus buildRuleStatus(RateLimitingRuleData data) {
    EdgeDecisionRuleStatus.Builder builder = EdgeDecisionRuleStatus.newBuilder();
    builder.setDisabled(!data.getEnabled());
    RuleStatus status = data.getRuleStatus();
    builder.setInternal(status.getInternal());
    return builder.build();
  }

  private Optional<EdgeDecisionRuleScope> buildRuleScope(RateLimitingRuleData data) {
    RuleConfigScope scope = data.getRuleConfigScope();
    EdgeDecisionRuleScope.Builder builder = EdgeDecisionRuleScope.newBuilder();
    if (scope.hasEnvironmentScope()
        && !scope.getEnvironmentScope().getEnvironmentIdsList().isEmpty()) {
      builder.addScopeConditions(
          EdgeDecisionRuleScopeCondition.newBuilder()
              .setEnvironmentScope(
                  EnvironmentScope.newBuilder()
                      .addAllEnvironments(scope.getEnvironmentScope().getEnvironmentIdsList())));
    }
    return builder.getScopeConditionsList().isEmpty()
        ? Optional.empty()
        : Optional.of(builder.build());
  }

  private EdgeDecision buildRuleDecision(RateLimitingRuleData data) {
    EdgeDecision.Builder builder = EdgeDecision.newBuilder();
    return builder.build();
  }

  private EdgeDecisionRuleDefinition buildRuleDefinition(RateLimitingRuleData data) {
    EdgeDecisionRuleDefinition.Builder builder = EdgeDecisionRuleDefinition.newBuilder();
    return builder.build();
  }
}
