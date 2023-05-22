package ai.traceable.external.agent.attribute.config.service.translator.servicenaming;

import static ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.ComparisonOperator.COMPARISON_OPERATOR_EQUALS;
import static ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX;
import static ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.LogicalOperator.LOGICAL_OPERATOR_AND;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Action;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.AttributePredicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.ComparisonOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.LogicalPredicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.StringPredicate;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRule;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleAction;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleCondition;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleCondition.AttributeCondition;
import ai.traceable.span.processing.config.service.v1.ServiceNamingRuleCondition.ConditionMatchOperator;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = @Inject)
public class ServiceNamingRuleTranslator {
  private static final String SERVICE_NAME_OVERRIDE_ATTRIBUTE = "traceableai.service.name";

  private static final String SERVICE_NAMING_RULE_ATTRIBUTE = "traceableai.servicenaming.rule";
  private final AttributeRuleBuilder attributeRuleBuilder;

  public Optional<AttributeRule> buildRule(List<ServiceNamingRule> serviceNamingRules) {
    List<AttributeRule> rules =
        serviceNamingRules.stream()
            .filter(ServiceNamingRule::getEnabled)
            .sorted(Comparator.comparingInt(ServiceNamingRule::getRank))
            .map(this::buildRule)
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableList());

    return rules.isEmpty()
        ? Optional.empty()
        : Optional.of(this.attributeRuleBuilder.buildRuleForFirstMatchingProjector(rules));
  }

  private Optional<AttributeRule> buildRule(ServiceNamingRule rule) {
    return this.translateConditions(rule.getConditionsList(), rule.getId())
        .flatMap(predicate -> this.buildConditionalRule(predicate, rule));
  }

  private Optional<AttributeRule> buildConditionalRule(
      Predicate predicate, ServiceNamingRule rule) {
    return this.buildActionRule(rule)
        .map(
            actionRule ->
                AttributeRule.newBuilder()
                    .setProjector(
                        Projector.newBuilder()
                            .setConditionalProjector(
                                ConditionalProjector.newBuilder()
                                    .setPredicate(predicate)
                                    .setAttributeRule(actionRule)))
                    .build());
  }

  private Optional<AttributeRule> buildActionRule(ServiceNamingRule rule) {
    return this.buildServiceNameReplacementAction(rule.getAction(), rule.getId())
        .map(
            serviceNameAction ->
                AttributeRule.newBuilder()
                    .addInitialActions(serviceNameAction)
                    .addInitialActions(
                        attributeRuleBuilder.buildAttributeAdditionAction(
                            SERVICE_NAMING_RULE_ATTRIBUTE, rule.getId()))
                    .build());
  }

  private Optional<Action> buildServiceNameReplacementAction(
      ServiceNamingRuleAction action, String ruleId) {
    switch (action.getActionCase()) {
      case STATIC_NAME_ASSIGNMENT:
        return Optional.of(
            attributeRuleBuilder.buildAttributeAdditionAction(
                SERVICE_NAME_OVERRIDE_ATTRIBUTE,
                action.getStaticNameAssignment().getServiceName()));
      case ACTION_NOT_SET:
      default:
        log.error("Dropping Rule {}: Unsupported ServiceNamingRuleAction {}", ruleId, action);
        return Optional.empty();
    }
  }

  private Optional<Predicate> translateConditions(
      List<ServiceNamingRuleCondition> conditions, String ruleId) {
    if (conditions.isEmpty()) {
      log.error("Dropping Rule {}: Service naming rule has no conditions.", ruleId);
      return Optional.empty();
    }

    List<Predicate> translatedConditions =
        conditions.stream()
            .map(condition -> this.translateCondition(condition, ruleId))
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableList());

    if (conditions.size() != translatedConditions.size()) {
      // At least one condition was dropped. Will have logged the specific reason when dropping.
      return Optional.empty();
    }

    if (translatedConditions.size() == 1) {
      return Optional.of(translatedConditions.get(0));
    }

    return Optional.of(
        Predicate.newBuilder()
            .setLogicalPredicate(
                LogicalPredicate.newBuilder()
                    .setOperator(LOGICAL_OPERATOR_AND)
                    .addAllChildren(translatedConditions))
            .build());
  }

  private Optional<Predicate> translateCondition(
      ServiceNamingRuleCondition condition, String ruleId) {
    switch (condition.getConditionCase()) {
      case ATTRIBUTE_CONDITION:
        return this.translateAttributeCondition(condition.getAttributeCondition(), ruleId)
            .map(
                attributePredicate ->
                    Predicate.newBuilder().setAttributePredicate(attributePredicate))
            .map(Predicate.Builder::build);
      case CONDITION_NOT_SET:
      default:
        log.error("Dropping Rule {}: Unsupported condition {}", ruleId, condition);
        return Optional.empty();
    }
  }

  private Optional<AttributePredicate> translateAttributeCondition(
      AttributeCondition attributeCondition, String ruleId) {
    return this.translateOperator(attributeCondition.getOperator(), ruleId)
        .map(
            operator ->
                AttributePredicate.newBuilder()
                    .setNamePredicate(
                        StringPredicate.newBuilder()
                            .setOperator(COMPARISON_OPERATOR_EQUALS)
                            .setValue(attributeCondition.getAttributeKey()))
                    .setValuePredicate(
                        StringPredicate.newBuilder()
                            .setOperator(operator)
                            .setValue(attributeCondition.getValue())))
        .map(AttributePredicate.Builder::build);
  }

  private Optional<ComparisonOperator> translateOperator(
      ConditionMatchOperator operator, String ruleId) {
    switch (operator) {
      case CONDITION_MATCH_OPERATOR_EQUALS:
        return Optional.of(COMPARISON_OPERATOR_EQUALS);
      case CONDITION_MATCH_OPERATOR_MATCHES_REGEX:
        return Optional.of(COMPARISON_OPERATOR_MATCHES_REGEX);
      case UNRECOGNIZED:
      case CONDITION_MATCH_OPERATOR_UNSPECIFIED:
      default:
        log.error("Dropping Rule {}: Unsupported condition operator {}", ruleId, operator);
        return Optional.empty();
    }
  }
}
