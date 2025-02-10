package ai.traceable.customsignature.config.service.rules;

import static ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategory.EDGE_DECISION_RULE_CATEGORY_CUSTOM_SIGNATURE;

import ai.traceable.customsignature.config.service.v1.CustomSignatureRule;
import ai.traceable.customsignature.config.service.v1.RuleScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecision;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionEngineConfig;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleDefinition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleStatus;
import ai.traceable.edge.decision.config.service.v1.EnvironmentScope;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CustomSignatureEdgeDecisionConverter {

  public EdgeDecisionEngineConfig convert(List<CustomSignatureRule> customSignatureRules) {
    EdgeDecisionEngineConfig.Builder builder = EdgeDecisionEngineConfig.newBuilder();
    List<EdgeDecisionRule> edgeDecisionRules =
        customSignatureRules.stream()
            .map(this::convertCustomSignatureRule)
            .collect(Collectors.toUnmodifiableList());
    builder.addAllDecisionRules(edgeDecisionRules);
    return builder.build();
  }

  private EdgeDecisionRule convertCustomSignatureRule(CustomSignatureRule customSignatureRule) {
    EdgeDecisionRule.Builder builder = EdgeDecisionRule.newBuilder();
    builder.setId(customSignatureRule.getId());
    builder.setName(customSignatureRule.getName());
    builder.setRuleStatus(buildRuleStatus(customSignatureRule));
    builder.setRuleCategory(EDGE_DECISION_RULE_CATEGORY_CUSTOM_SIGNATURE);
    buildRuleScope(customSignatureRule).ifPresent(builder::setRuleScope);
    builder.setRuleDecision(buildRuleDecision(customSignatureRule));
    builder.setRuleDefinition(buildRuleDefinition(customSignatureRule));
    return builder.build();
  }

  private EdgeDecisionRuleStatus buildRuleStatus(CustomSignatureRule customSignatureRule) {
    EdgeDecisionRuleStatus.Builder builder = EdgeDecisionRuleStatus.newBuilder();
    builder.setDisabled(customSignatureRule.getDisabled());
    builder.setInternal(customSignatureRule.getInternal());
    return builder.build();
  }

  private Optional<EdgeDecisionRuleScope> buildRuleScope(CustomSignatureRule customSignatureRule) {
    RuleScope scope = customSignatureRule.getRuleScope();
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

  private EdgeDecision buildRuleDecision(CustomSignatureRule customSignatureRule) {
    EdgeDecision.Builder builder = EdgeDecision.newBuilder();
    return builder.build();
  }

  private EdgeDecisionRuleDefinition buildRuleDefinition(CustomSignatureRule customSignatureRule) {
    EdgeDecisionRuleDefinition.Builder builder = EdgeDecisionRuleDefinition.newBuilder();
    return builder.build();
  }
}
