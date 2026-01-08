package ai.traceable.edge.decision.config.service.store;

import ai.traceable.config.commons.v1.AuditFilter;
import ai.traceable.config.commons.v1.TimestampRange;
import ai.traceable.config.utils.TimestampConverter;
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
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Optional;
import org.hypertrace.config.objectstore.ContextualConfigObject;

public class FilterEvaluator {

  private final TimestampConverter timestampConverter;

  @Inject
  public FilterEvaluator(TimestampConverter timestampConverter) {
    this.timestampConverter = timestampConverter;
  }

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
      case AUDIT_FILTER:
        return evaluate(filter.getAuditFilter(), ruleWithContext);
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

  private boolean evaluate(
      AuditFilter auditFilter, ContextualConfigObject<EdgeDecisionRule> ruleWithContext) {
    return matchesCreatedRange(ruleWithContext, auditFilter)
        && matchesUpdatedRange(ruleWithContext, auditFilter)
        && matchesCreatedByContains(ruleWithContext, auditFilter)
        && matchesLastUpdatedByContains(ruleWithContext, auditFilter);
  }

  private boolean matchesCreatedRange(
      ContextualConfigObject<EdgeDecisionRule> configObject, AuditFilter filter) {
    if (!filter.hasCreatedRange()) {
      return true;
    }
    Instant creationTimestamp = configObject.getCreationTimestamp();
    if (creationTimestamp == null) {
      return false;
    }
    return isTimestampInRange(creationTimestamp, filter.getCreatedRange());
  }

  private boolean matchesUpdatedRange(
      ContextualConfigObject<EdgeDecisionRule> configObject, AuditFilter filter) {
    if (!filter.hasUpdatedRange()) {
      return true;
    }
    Instant lastUserUpdateTimestamp = configObject.getLastUserUpdateTimestamp();
    if (lastUserUpdateTimestamp == null) {
      return false;
    }
    return isTimestampInRange(lastUserUpdateTimestamp, filter.getUpdatedRange());
  }

  private boolean matchesCreatedByContains(
      ContextualConfigObject<EdgeDecisionRule> configObject, AuditFilter filter) {
    if (filter.getCreatedByContains().isEmpty()) {
      return true;
    }
    String createdByEmail = configObject.getCreatedByEmail();
    if (createdByEmail == null || createdByEmail.isEmpty()) {
      return false;
    }
    return createdByEmail.toLowerCase().contains(filter.getCreatedByContains().toLowerCase());
  }

  private boolean matchesLastUpdatedByContains(
      ContextualConfigObject<EdgeDecisionRule> configObject, AuditFilter filter) {
    if (filter.getLastUpdatedByUserContains().isEmpty()) {
      return true;
    }
    String lastUserUpdateEmail = configObject.getLastUserUpdateEmail();
    if (lastUserUpdateEmail == null || lastUserUpdateEmail.isEmpty()) {
      return false;
    }
    return lastUserUpdateEmail
        .toLowerCase()
        .contains(filter.getLastUpdatedByUserContains().toLowerCase());
  }

  private boolean isTimestampInRange(Instant timestamp, TimestampRange range) {
    if (range.hasStart()) {
      Instant startTime = timestampConverter.convertToInstant(range.getStart());
      if (timestamp.isBefore(startTime)) {
        return false;
      }
    }
    if (range.hasEnd()) {
      Instant endTime = timestampConverter.convertToInstant(range.getEnd());
      return !timestamp.isAfter(endTime);
    }
    return true;
  }
}
