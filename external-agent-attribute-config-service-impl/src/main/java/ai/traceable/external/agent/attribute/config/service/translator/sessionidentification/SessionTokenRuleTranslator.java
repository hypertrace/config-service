package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Action;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Action.AttributeAddition;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ConditionalProjector;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.EachMatchingProjector;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenRule;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
class SessionTokenRuleTranslator {
  private final PredicateTranslator predicateTranslator;
  private final ExpirationTranslator expirationTranslator;
  private final ProjectionRootTranslator projectionRootTranslator;
  private final SessionIdentificationConstants sessionIdentificationConstants;

  AttributeRule translateSessionTokenRule(
      SessionTokenRule tokenRule, int ruleIndex, String ruleId) {
    AttributeRule.Builder ruleBuilder = AttributeRule.newBuilder();
    Projector projector = projectionRootTranslator.translateForTokenValue(tokenRule);
    if (tokenRule.hasRequestSessionTokenDetails()) {
      ruleBuilder.addInitialActions(
          addAttributeAndProject(
              sessionIdentificationConstants.buildKeyForSessionId(ruleId, ruleIndex), projector));
    } else if (tokenRule.getResponseSessionTokenDetails().hasResponseAttributeExpiration()) {
      ruleBuilder
          .setProjector(
              Projector.newBuilder()
                  .setEachMatchingProjector(
                      EachMatchingProjector.newBuilder()
                          .addAttributeRules(
                              AttributeRule.newBuilder()
                                  .addInitialActions(
                                      addAttributeAndProject(
                                          sessionIdentificationConstants.buildKeyForNewSessionId(
                                              ruleId, ruleIndex),
                                          projector)))
                          .addAttributeRules(
                              expirationTranslator.translateExpiration(
                                  tokenRule.getResponseSessionTokenDetails(),
                                  ruleIndex,
                                  ruleId,
                                  tokenRule
                                      .getTokenValueRule()
                                      .getTokenValueProjection()
                                      .getAttributeProjection()
                                      .getAttributeKeyMatchCondition()))))
          .build();
    } else {
      ruleBuilder.addInitialActions(
          addAttributeAndProject(
              sessionIdentificationConstants.buildKeyForNewSessionId(ruleId, ruleIndex),
              projector));
    }

    if (tokenRule.hasTokenConditionalPredicate()) {
      return AttributeRule.newBuilder()
          .setProjector(
              Projector.newBuilder()
                  .setConditionalProjector(
                      ConditionalProjector.newBuilder()
                          .setPredicate(
                              predicateTranslator.translatePredicate(
                                  tokenRule.getTokenConditionalPredicate()))
                          .setAttributeRule(ruleBuilder)))
          .build();
    }
    return ruleBuilder.build();
  }

  private Action addAttributeAndProject(String attributeKey, Projector projector) {
    return Action.newBuilder()
        .setAttributeAddition(
            AttributeAddition.newBuilder()
                .setAttributeKey(attributeKey)
                .setValueProjectionRule(AttributeRule.newBuilder().setProjector(projector)))
        .build();
  }
}
