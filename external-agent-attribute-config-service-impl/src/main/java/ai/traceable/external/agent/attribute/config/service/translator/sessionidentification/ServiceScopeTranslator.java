package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.AttributePredicate;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.ComparisonOperator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector.Predicate.StringPredicate;
import java.util.List;

class ServiceScopeTranslator {
  Predicate addServiceScopeRegexes(List<String> serviceScopesRegexes) {
    String regexValue = String.join("|", serviceScopesRegexes);
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
