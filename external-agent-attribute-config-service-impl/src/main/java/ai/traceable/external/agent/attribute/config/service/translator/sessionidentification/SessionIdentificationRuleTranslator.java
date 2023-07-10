package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationRule;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenRule;
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
    return sessionIdentificationRules.stream().map(this::translateRule);
  }

  private AttributeRule translateRule(SessionIdentificationRule rule) {
    int ruleIndex = 0;
    Projector.EachMatchingProjector.Builder projector =
        Projector.EachMatchingProjector.newBuilder();
    for (SessionTokenRule tokenRule : rule.getTokenRulesList()) {
      projector.addAttributeRules(
          sessionTokenRuleTranslator.translateSessionTokenRule(tokenRule, ruleIndex, rule.getId()));
      ruleIndex++;
    }
    return predicateTranslator.addScopePredicatesIfSet(
        rule.getScope(),
        AttributeRule.newBuilder()
            .setProjector(Projector.newBuilder().setEachMatchingProjector(projector))
            .build());
  }
}
