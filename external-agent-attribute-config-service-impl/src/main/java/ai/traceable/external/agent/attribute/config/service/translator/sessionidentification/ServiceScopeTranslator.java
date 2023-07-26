package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.AttributePredicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.ComparisonOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.StringPredicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

class ServiceScopeTranslator {
  Optional<Predicate> addServiceScopes(List<String> serviceNameRegexes, List<String> serviceNames) {
    List<String> serviceScopeRegexes = new ArrayList<>();
    if (!serviceNameRegexes.isEmpty()) {
      serviceScopeRegexes.addAll(serviceNameRegexes);
    }
    if (!serviceNames.isEmpty()) {
      serviceNames.forEach(name -> serviceScopeRegexes.add("^" + name + "$"));
    }

    if (!serviceScopeRegexes.isEmpty()) {
      return Optional.of(buildPredicate(String.join("|", serviceScopeRegexes)));
    }
    return Optional.empty();
  }

  private Predicate buildPredicate(String regexValue) {
    return Predicate.newBuilder()
        .setAttributePredicate(
            AttributePredicate.newBuilder()
                .setNamePredicate(
                    StringPredicate.newBuilder()
                        .setOperator(ComparisonOperator.COMPARISON_OPERATOR_EQUALS)
                        .setValue("service.name"))
                .setValuePredicate(
                    StringPredicate.newBuilder()
                        .setOperator(ComparisonOperator.COMPARISON_OPERATOR_MATCHES_REGEX)
                        .setValue(regexValue)))
        .build();
  }
}
