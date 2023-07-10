package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.URL_KEYS;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.ComparisonOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.LogicalOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.LogicalPredicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.ProjectorPredicate;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRuleScope;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
class PredicateTranslator {
  private final AttributePredicateTranslator attributePredicateTranslator;
  private final ServiceScopeTranslator serviceScopeTranslator;
  private final AttributeRuleBuilder attributeRuleBuilder;
  private final CustomProjectionTranslator customProjectionTranslator;

  Predicate translatePredicate(
      ai.traceable.sessionidentification.config.service.v1.Predicate predicate) {
    if (predicate.hasCustomPredicate()) {
      return Predicate.newBuilder()
          .setProjectorPredicate(
              ProjectorPredicate.newBuilder()
                  .setProjector(
                      customProjectionTranslator.translateCustomProjection(
                          predicate.getCustomPredicate())))
          .build();
    }

    List<Projector> differentLocationPredicateProjectors =
        attributePredicateTranslator.translate(predicate.getAttributePredicate());

    if (differentLocationPredicateProjectors.size() == 1) {
      return Predicate.newBuilder()
          .setProjectorPredicate(
              ProjectorPredicate.newBuilder()
                  .setProjector(differentLocationPredicateProjectors.get(0)))
          .build();
    }

    return Predicate.newBuilder()
        .setLogicalPredicate(
            LogicalPredicate.newBuilder()
                .setOperator(LogicalOperator.LOGICAL_OPERATOR_OR)
                .addAllChildren(
                    differentLocationPredicateProjectors.stream()
                        .map(
                            projector ->
                                Predicate.newBuilder()
                                    .setProjectorPredicate(
                                        ProjectorPredicate.newBuilder().setProjector(projector))
                                    .build())
                        .collect(Collectors.toUnmodifiableList())))
        .build();
  }

  AttributeRule addScopePredicatesIfSet(
      SessionIdentificationRuleScope scope, AttributeRule attributeRule) {
    return buildScopePredicate(scope)
        .map(
            predicate ->
                AttributeRule.newBuilder()
                    .setProjector(
                        Projector.newBuilder()
                            .setConditionalProjector(
                                ConditionalProjector.newBuilder()
                                    .setPredicate(predicate)
                                    .setAttributeRule(attributeRule)))
                    .build())
        .orElse(attributeRule);
  }

  private Optional<Predicate> buildScopePredicate(SessionIdentificationRuleScope scope) {
    if (scope.getUrlMatchRegexesCount() != 0 && scope.getServiceNameRegexesCount() != 0) {
      return Optional.of(
          Predicate.newBuilder()
              .setLogicalPredicate(
                  LogicalPredicate.newBuilder()
                      .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                      .addChildren(
                          attributeRuleBuilder.buildPredicate(
                              ComparisonOperator.COMPARISON_OPERATOR_EQUALS,
                              URL_KEYS,
                              ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX,
                              String.join("|", scope.getUrlMatchRegexesList())))
                      .addChildren(
                          serviceScopeTranslator.addServiceScopeRegexes(
                              scope.getServiceNameRegexesList())))
              .build());
    } else if (scope.getUrlMatchRegexesCount() != 0) {
      return Optional.of(
          attributeRuleBuilder.buildPredicate(
              ComparisonOperator.COMPARISON_OPERATOR_EQUALS,
              URL_KEYS,
              ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX,
              String.join("|", scope.getUrlMatchRegexesList())));
    } else if (scope.getServiceNameRegexesCount() != 0) {
      return Optional.of(
          serviceScopeTranslator.addServiceScopeRegexes(scope.getServiceNameRegexesList()));
    }
    return Optional.empty();
  }
}
