package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import static ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.SessionIdentificationConstants.SESSION_ATTRIBUTE_REGEX;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.sessionidentification.config.service.v1.RuleCreationSource;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenRule;
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class SessionIdentificationRuleTranslator {
  private final SessionTokenRuleTranslator sessionTokenRuleTranslator;
  private final PredicateTranslator predicateTranslator;

  public Stream<AttributeRule> translateSessionIdentificationRules(
      List<SessionIdentificationRule> sessionIdentificationRules) {
    List<AttributeRule> translatedSessionIdentificationRulesV1 = new ArrayList<>();
    List<AttributeRule> translatedSessionIdentificationRulesV2 = new ArrayList<>();
    sessionIdentificationRules.forEach(
        rule -> {
          if (rule.getStatus()
              .getRuleCreationSource()
              .equals(RuleCreationSource.RULE_CREATION_SOURCE_OLD_API)) {
            translatedSessionIdentificationRulesV1.add(this.translateRuleForOldApi(rule));
          } else {
            translatedSessionIdentificationRulesV2.add(this.translateRule(rule));
          }
        });
    if (translatedSessionIdentificationRulesV1.isEmpty()) {
      return translatedSessionIdentificationRulesV2.stream();
    }

    return Streams.concat(
        Stream.of(
            AttributeRule.newBuilder()
                .setProjector(
                    Projector.newBuilder()
                        .setEachMatchingProjector(
                            Projector.EachMatchingProjector.newBuilder()
                                .addAllAttributeRules(translatedSessionIdentificationRulesV1)))
                .build()),
        translatedSessionIdentificationRulesV2.stream());
  }

  private AttributeRule translateRule(SessionIdentificationRule rule) {
    int ruleIndex = 0;
    Projector.EachMatchingProjector.Builder projector =
        Projector.EachMatchingProjector.newBuilder();
    for (SessionTokenRule tokenRule : rule.getTokenRulesList()) {
      projector.addAttributeRules(
          sessionTokenRuleTranslator.translateSessionTokenRule(
              tokenRule, ruleIndex, rule.getId(), rule.getStatus().getRuleCreationSource()));
      ruleIndex++;
    }
    return predicateTranslator.addScopePredicatesIfSet(
        rule.getScope(),
        AttributeRule.newBuilder()
            .setProjector(Projector.newBuilder().setEachMatchingProjector(projector))
            .build());
  }

  private AttributeRule translateRuleForOldApi(SessionIdentificationRule rule) {
    // old api would have only single rule
    AttributeRule attributeRule =
        predicateTranslator.addScopePredicatesIfSet(
            rule.getScope(),
            sessionTokenRuleTranslator.translateSessionTokenRule(
                rule.getTokenRules(0), 0, rule.getId(), rule.getStatus().getRuleCreationSource()));
    return AttributeRule.newBuilder()
        .setProjector(
            Projector.newBuilder()
                .setConditionalProjector(
                    Projector.ConditionalProjector.newBuilder()
                        .setPredicate(
                            Projector.ConditionalProjector.Predicate.newBuilder()
                                .setAttributePredicate(
                                    Projector.ConditionalProjector.Predicate.AttributePredicate
                                        .newBuilder()
                                        .setNamePredicate(
                                            Projector.ConditionalProjector.Predicate.StringPredicate
                                                .newBuilder()
                                                .setOperator(
                                                    Projector.ConditionalProjector.Predicate
                                                        .ComparisonOperator
                                                        .COMPARISON_OPERATOR_NOT_MATCHES_REGEX)
                                                .setValue(SESSION_ATTRIBUTE_REGEX))))
                        .setAttributeRule(attributeRule)))
        .build();
  }
}
