package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Action;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Action.AttributeAddition;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.sessionidentification.config.service.v1.RequestSessionTokenDetails;
import ai.traceable.sessionidentification.config.service.v1.ResponseSessionTokenDetails;
import ai.traceable.sessionidentification.config.service.v1.RuleCreationSource;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenRule;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
class SessionTokenRuleTranslator {
  private final PredicateTranslator predicateTranslator;
  private final ExpirationTranslator expirationTranslator;
  private final ProjectionRootTranslator projectionRootTranslator;
  private final SessionIdentificationConstants sessionIdentificationConstants;

  AttributeRule translateSessionTokenRule(
      SessionTokenRule tokenRule,
      int ruleIndex,
      String ruleId,
      RuleCreationSource ruleCreationSource) {

    List<Projector> projectors =
        projectionRootTranslator.translateForTokenValue(tokenRule, ruleCreationSource);
    switch (tokenRule.getTokenTypeCase()) {
      case RESPONSE_SESSION_TOKEN_DETAILS:
        if (tokenRule.getResponseSessionTokenDetails().getExpirationCase()
            != ResponseSessionTokenDetails.ExpirationCase.EXPIRATION_NOT_SET) {
          return predicateTranslator.addConditionalPredicateIfPresent(
              buildAttributeRule(
                  sessionIdentificationConstants.buildKeyForNewSessionId(ruleId, ruleIndex),
                  projectors,
                  expirationTranslator.translateExpiration(
                      tokenRule.getResponseSessionTokenDetails(),
                      ruleIndex,
                      ruleId,
                      tokenRule
                          .getTokenValueRule()
                          .getTokenValueProjection()
                          .getAttributeProjection())),
              tokenRule);
        }

        return addConditionalPredicateIfPresentAndReturnAttributeRule(
            projectors,
            sessionIdentificationConstants.buildKeyForNewSessionId(ruleId, ruleIndex),
            tokenRule);
      case REQUEST_SESSION_TOKEN_DETAILS:
        if (tokenRule.getRequestSessionTokenDetails().getExpirationCase()
            != RequestSessionTokenDetails.ExpirationCase.EXPIRATION_NOT_SET) {
          return predicateTranslator.addConditionalPredicateIfPresent(
              buildAttributeRule(
                  sessionIdentificationConstants.buildKeyForSessionId(ruleId, ruleIndex),
                  projectors,
                  expirationTranslator.translateExpiration(
                      tokenRule.getRequestSessionTokenDetails(),
                      ruleIndex,
                      ruleId,
                      tokenRule
                          .getTokenValueRule()
                          .getTokenValueProjection()
                          .getAttributeProjection())),
              tokenRule);
        }
        return addConditionalPredicateIfPresentAndReturnAttributeRule(
            projectors,
            sessionIdentificationConstants.buildKeyForSessionId(ruleId, ruleIndex),
            tokenRule);
      default:
        // for custom projection, we don't know the case, request or response
        // so keeping the attribute name starting with traceableai.session<>
        return addConditionalPredicateIfPresentAndReturnAttributeRule(
            projectors,
            sessionIdentificationConstants.buildKeyForSessionId(ruleId, ruleIndex),
            tokenRule);
    }
  }

  private AttributeRule addConditionalPredicateIfPresentAndReturnAttributeRule(
      List<Projector> projectors, String sessionIdAttr, SessionTokenRule tokenRule) {
    return predicateTranslator.addConditionalPredicateIfPresent(
        projectors.stream()
            .map(projector -> addAttributeAndProject(sessionIdAttr, projector))
            .collect(Collectors.toUnmodifiableList()),
        tokenRule);
  }

  private AttributeRule buildAttributeRule(
      String sessionIdKey, List<Projector> projectors, List<Action> expirationActions) {
    return AttributeRule.newBuilder()
        .setProjector(
            Projector.newBuilder()
                .setEachMatchingProjector(
                    Projector.EachMatchingProjector.newBuilder()
                        .addAttributeRules(
                            predicateTranslator.buildFirstMatchingProjector(
                                projectors.stream()
                                    .map(
                                        projector ->
                                            AttributeRule.newBuilder()
                                                .addInitialActions(
                                                    addAttributeAndProject(sessionIdKey, projector))
                                                .build())
                                    .collect(Collectors.toUnmodifiableList())))
                        .addAttributeRules(
                            predicateTranslator.buildFirstMatchingProjector(
                                expirationActions.stream()
                                    .map(
                                        action ->
                                            AttributeRule.newBuilder()
                                                .addInitialActions(action)
                                                .build())
                                    .collect(Collectors.toUnmodifiableList())))))
        .build();
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
