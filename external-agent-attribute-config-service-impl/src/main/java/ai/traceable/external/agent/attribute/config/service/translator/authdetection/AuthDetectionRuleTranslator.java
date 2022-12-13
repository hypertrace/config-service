package ai.traceable.external.agent.attribute.config.service.translator.authdetection;

import ai.traceable.auth.detection.config.service.v1.AuthDetectionRule;
import ai.traceable.auth.detection.config.service.v1.Predicate;
import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector;
import java.util.Optional;
import java.util.stream.Stream;

public abstract class AuthDetectionRuleTranslator {
  protected final AttributeRuleBuilder attributeRuleBuilder;

  AuthDetectionRuleTranslator(AttributeRuleBuilder attributeRuleBuilder) {
    this.attributeRuleBuilder = attributeRuleBuilder;
  }

  abstract Predicate.PredicateCase getPredicateCase();

  abstract Optional<ConditionalProjector.Predicate> translatePredicate(Predicate predicate);

  public Stream<AttributeRule> translateRuleForAuthType(AuthDetectionRule rule) {
    return this.translatePredicate(rule.getPredicate())
        .map(
            translatedPredicate ->
                AttributeRule.newBuilder()
                    .setProjector(
                        Projector.newBuilder()
                            .setConditionalProjector(
                                ConditionalProjector.newBuilder()
                                    .setPredicate(translatedPredicate)
                                    .setAttributeRule(
                                        this.attributeRuleBuilder
                                            .buildActionAttributeRuleForAuthType(
                                                rule.getAuthType(), rule.getId()))))
                    .build())
        .stream();
  }
}
