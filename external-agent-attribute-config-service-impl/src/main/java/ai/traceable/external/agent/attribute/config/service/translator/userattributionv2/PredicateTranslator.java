package ai.traceable.external.agent.attribute.config.service.translator.userattributionv2;

import static ai.traceable.config.utils.RegexUtils.escapeRegex;
import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.URL_OR_PATH_ATTRIBUTE_KEYS;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.LogicalOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.LogicalPredicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.ProjectorPredicate;
import ai.traceable.userattribution.config.service.v2.UserAttributionRootTokenRule;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleData;
import ai.traceable.userattribution.config.service.v2.UserAttributionRuleScope;
import ai.traceable.userattribution.config.service.v2.UserAttributionTokenRule;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
class PredicateTranslator {
  private static final String TRACEABLEAI_SERVICE_NAME_ATTRIBUTE = "traceableai.service.name";
  private static final String SERVICE_NAME_ATTRIBUTE = "service.name";

  private final AttributeTranslator attributeTranslator;
  private final AttributeRuleBuilder attributeRuleBuilder;

  AttributeRule addConditionalPredicateIfPresent(
      UserAttributionTokenRule tokenRule, AttributeRule childAttributeRule) {
    if (!tokenRule.hasTokenConditionalPredicate()) {
      return childAttributeRule;
    }
    return AttributeRule.newBuilder()
        .setProjector(
            addConditionalPredicate(tokenRule.getTokenConditionalPredicate(), childAttributeRule))
        .build();
  }

  AttributeRule addConditionalPredicateIfPresent(
      UserAttributionRootTokenRule rootTokenRule, AttributeRule childAttributeRule) {
    if (!rootTokenRule.hasTokenConditionalPredicate()) {
      return childAttributeRule;
    }
    return AttributeRule.newBuilder()
        .setProjector(
            addConditionalPredicate(
                rootTokenRule.getTokenConditionalPredicate(), childAttributeRule))
        .build();
  }

  Projector addConditionalPredicate(
      ai.traceable.userattribution.config.service.v2.Predicate predicate,
      AttributeRule childAttributeRule) {
    return Projector.newBuilder()
        .setConditionalProjector(
            ConditionalProjector.newBuilder()
                .setPredicate(translatePredicate(predicate))
                .setAttributeRule(childAttributeRule))
        .build();
  }

  private Predicate translatePredicate(
      ai.traceable.userattribution.config.service.v2.Predicate predicate) {
    if (predicate.hasLogicalPredicate()) {
      return Predicate.newBuilder()
          .setLogicalPredicate(
              LogicalPredicate.newBuilder()
                  .setOperator(convert(predicate.getLogicalPredicate().getOperator()))
                  .addAllChildren(
                      predicate.getLogicalPredicate().getChildrenList().stream()
                          .map(this::translatePredicate)
                          .collect(Collectors.toUnmodifiableList())))
          .build();
    }

    List<Projector> differentLocationPredicateProjectors =
        attributeTranslator.translate(predicate.getAttributePredicate());

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

  LogicalOperator convert(
      ai.traceable.userattribution.config.service.v2.Predicate.LogicalOperator operator) {
    switch (operator) {
      case LOGICAL_OPERATOR_AND:
        return LogicalOperator.LOGICAL_OPERATOR_AND;
      case LOGICAL_OPERATOR_OR:
        return LogicalOperator.LOGICAL_OPERATOR_OR;
      default:
        throw Status.INTERNAL
            .withDescription(String.format("Unknown logical operator: %s", operator))
            .asRuntimeException();
    }
  }

  Optional<Predicate> buildScopePredicate(UserAttributionRuleData data) {
    // return empty result
    // if there is no scope configured
    // OR there is no url scope or service scope configure(this implies there was environment scope
    // configured which was taken care off as part of user attribution rules query)
    if (!data.hasScope() || !(data.getScope().hasUrlScope() || data.getScope().hasServiceScope())) {
      return Optional.empty();
    }
    UserAttributionRuleScope scope = data.getScope();
    Optional<Predicate> serviceScopePredicate =
        scope.hasServiceScope()
            ? addServiceScopes(
                scope.getServiceScope().getServiceNameRegexes().getValuesList(),
                scope.getServiceScope().getServiceNames().getValuesList())
            : Optional.empty();
    Optional<Predicate> urlScopePredicate =
        scope.hasUrlScope()
            ? Optional.of(
                attributeRuleBuilder.buildPredicate(
                    Predicate.ComparisonOperator.COMPARISON_OPERATOR_EQUALS,
                    URL_OR_PATH_ATTRIBUTE_KEYS,
                    Predicate.ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX,
                    String.join("|", scope.getUrlScope().getUrlMatchRegexes().getValuesList())))
            : Optional.empty();

    if (serviceScopePredicate.isPresent() && urlScopePredicate.isPresent()) {
      Predicate.newBuilder()
          .setLogicalPredicate(
              LogicalPredicate.newBuilder()
                  .setOperator(LogicalOperator.LOGICAL_OPERATOR_AND)
                  .addChildren(serviceScopePredicate.get())
                  .addChildren(urlScopePredicate.get())
                  .build());
    } else if (serviceScopePredicate.isPresent()) {
      return serviceScopePredicate;
    }

    return urlScopePredicate;
  }

  Optional<Predicate> addServiceScopes(List<String> serviceNameRegexes, List<String> serviceNames) {
    List<String> serviceScopeRegexes = new ArrayList<>();
    if (!serviceNameRegexes.isEmpty()) {
      serviceScopeRegexes.addAll(serviceNameRegexes);
    }
    if (!serviceNames.isEmpty()) {
      serviceNames.forEach(name -> serviceScopeRegexes.add("^" + escapeRegex(name) + "$"));
    }

    if (!serviceScopeRegexes.isEmpty()) {
      return Optional.of(buildPredicate(String.join("|", serviceScopeRegexes)));
    }
    return Optional.empty();
  }

  private Predicate buildPredicate(String regexValue) {
    return Predicate.newBuilder()
        .setLogicalPredicate(
            LogicalPredicate.newBuilder()
                .setOperator(LogicalOperator.LOGICAL_OPERATOR_OR)
                .addChildren(
                    Predicate.newBuilder()
                        .setAttributePredicate(
                            Predicate.AttributePredicate.newBuilder()
                                .setNamePredicate(
                                    Predicate.StringPredicate.newBuilder()
                                        .setOperator(
                                            Predicate.ComparisonOperator.COMPARISON_OPERATOR_EQUALS)
                                        .setValue(TRACEABLEAI_SERVICE_NAME_ATTRIBUTE))
                                .setValuePredicate(
                                    Predicate.StringPredicate.newBuilder()
                                        .setOperator(
                                            Predicate.ComparisonOperator
                                                .COMPARISON_OPERATOR_MATCHES_REGEX)
                                        .setValue(regexValue))))
                .addChildren(
                    Predicate.newBuilder()
                        .setAttributePredicate(
                            Predicate.AttributePredicate.newBuilder()
                                .setNamePredicate(
                                    Predicate.StringPredicate.newBuilder()
                                        .setOperator(
                                            Predicate.ComparisonOperator.COMPARISON_OPERATOR_EQUALS)
                                        .setValue(SERVICE_NAME_ATTRIBUTE))
                                .setValuePredicate(
                                    Predicate.StringPredicate.newBuilder()
                                        .setOperator(
                                            Predicate.ComparisonOperator
                                                .COMPARISON_OPERATOR_MATCHES_REGEX)
                                        .setValue(regexValue)))))
        .build();
  }
}
