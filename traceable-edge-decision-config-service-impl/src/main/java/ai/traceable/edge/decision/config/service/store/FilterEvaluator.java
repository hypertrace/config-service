package ai.traceable.edge.decision.config.service.store;

import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRule;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleCategoryFilter;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScope;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeCondition;
import ai.traceable.edge.decision.config.service.v1.EdgeDecisionRuleScopeFilter;
import ai.traceable.edge.decision.config.service.v1.Filter;
import ai.traceable.edge.decision.config.service.v1.GenericValueFilter;
import ai.traceable.edge.decision.config.service.v1.LogicalFilter;
import ai.traceable.edge.decision.config.service.v1.TimeRangeFilter;
import com.google.protobuf.Value;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import java.util.Optional;
import org.hypertrace.config.objectstore.ContextualConfigObject;

public class FilterEvaluator {

  public boolean evaluate(Filter filter, ContextualConfigObject<EdgeDecisionRule> ruleWithContext) {
    switch (filter.getFilterCase()) {
      case LOGICAL_FILTER:
        return evaluate(filter.getLogicalFilter(), ruleWithContext);
      case CUSTOM_FIELDS_FILTER:
        return evaluate(
            filter.getCustomFieldsFilter(),
            ruleWithContext.getData().getRuleDefinition().getCustomFields());
      case CREATION_TIME_FILTER:
        return evaluate(
            filter.getCreationTimeFilter(), ruleWithContext.getCreationTimestamp().toEpochMilli());
      case LAST_UPDATED_TIME_FILTER:
        return evaluate(
            filter.getLastUpdatedTimeFilter(),
            ruleWithContext.getLastUpdatedTimestamp().toEpochMilli());
      case EDGE_DECISION_RULE_CATEGORY_FILTER:
        return evaluate(filter.getEdgeDecisionRuleCategoryFilter(), ruleWithContext.getData());
      case INCLUDE_DISABLED:
        return evaluate(filter.getIncludeDisabled(), ruleWithContext.getData());
      case SCOPE_FILTER:
        return evaluate(filter.getScopeFilter(), ruleWithContext.getData().getRuleScope());
      case FILTER_NOT_SET:
        return true;
      default:
        throw Status.UNIMPLEMENTED
            .withDescription("Unimplemented filter case: " + filter.getFilterCase())
            .asRuntimeException();
    }
  }

  private boolean evaluate(
      LogicalFilter filter, ContextualConfigObject<EdgeDecisionRule> ruleWithContext) {
    switch (filter.getOperator()) {
      case LOGICAL_OPERATOR_OR:
        return filter.getFilterList().stream().anyMatch(f -> evaluate(f, ruleWithContext));
      case LOGICAL_OPERATOR_AND:
        return filter.getFilterList().stream().allMatch(f -> evaluate(f, ruleWithContext));
      case LOGICAL_OPERATOR_UNSPECIFIED:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unrecognized logical operator: " + filter.getOperator())
            .asRuntimeException();
    }
  }

  private boolean evaluate(GenericValueFilter filter, Value value) {
    if (!value.equals(Value.getDefaultInstance()) && !value.hasStructValue()) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              "GenericValueFilter can be applied on value fields having struct type only. Value kind: "
                  + value.getKindCase())
          .asRuntimeException();
    }
    if (!value.getStructValue().getFieldsMap().containsKey(filter.getKey())) {
      return false;
    }
    Value valueToMatch = value.getStructValue().getFieldsMap().get(filter.getKey());
    switch (filter.getOperator()) {
      case RELATIONAL_OPERATOR_EQUALS:
        return valueToMatch.equals(filter.getValue());
      case RELATIONAL_OPERATOR_UNSPECIFIED:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unrecognized relational operator: " + filter.getOperator())
            .asRuntimeException();
    }
  }

  private boolean evaluate(TimeRangeFilter filter, long timestampInMillis) {
    return Timestamps.toMillis(filter.getStartTime()) <= timestampInMillis
        && Timestamps.toMillis(filter.getEndTime()) >= timestampInMillis;
  }

  private boolean evaluate(
      EdgeDecisionRuleCategoryFilter edgeDecisionRuleCategoryFilter, EdgeDecisionRule rule) {
    return edgeDecisionRuleCategoryFilter
        .getEdgeDecisionRuleCategoriesList()
        .contains(rule.getRuleCategory());
  }

  private boolean evaluate(boolean includeDisabled, EdgeDecisionRule rule) {
    return includeDisabled || !rule.getRuleStatus().getDisabled();
  }

  private boolean evaluate(
      EdgeDecisionRuleScopeFilter scopeFilter, EdgeDecisionRuleScope ruleScope) {
    switch (scopeFilter.getScopeCase()) {
      case ENVIRONMENT_SCOPE:
        Optional<EdgeDecisionRuleScopeCondition> environmentScopeCondition =
            ruleScope.getScopeConditionsList().stream()
                .filter(EdgeDecisionRuleScopeCondition::hasEnvironmentScope)
                .findFirst();
        if (environmentScopeCondition.isEmpty()) {
          // no environment scope is defined => rule applies to all envs
          return true;
        }
        return environmentScopeCondition.get().getEnvironmentScope().getEnvironmentsList().stream()
            .anyMatch(env -> scopeFilter.getEnvironmentScope().getEnvironmentsList().contains(env));
      case SCOPE_NOT_SET:
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Unrecognized scope filter type: " + scopeFilter.getScopeCase())
            .asRuntimeException();
    }
  }
}
