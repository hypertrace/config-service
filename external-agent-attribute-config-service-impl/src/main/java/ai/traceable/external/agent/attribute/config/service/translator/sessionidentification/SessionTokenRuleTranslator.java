package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Action;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Action.AttributeAddition;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
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
      SessionTokenRule tokenRule, int ruleIndex, String ruleId) {

    List<Projector> projectors = projectionRootTranslator.translateForTokenValue(tokenRule);
    switch (tokenRule.getTokenTypeCase()) {
      case RESPONSE_SESSION_TOKEN_DETAILS:
        if (tokenRule.getResponseSessionTokenDetails().hasResponseAttributeExpiration()
            || tokenRule.getResponseSessionTokenDetails().hasJwtExpiration()) {
          return predicateTranslator.addConditionalPredicateIfPresent(
              AttributeRule.newBuilder()
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
                                                              addAttributeAndProject(
                                                                  sessionIdentificationConstants
                                                                      .buildKeyForNewSessionId(
                                                                          ruleId, ruleIndex),
                                                                  projector))
                                                          .build())
                                              .collect(Collectors.toUnmodifiableList())))
                                  .addAttributeRules(
                                      predicateTranslator.buildFirstMatchingProjector(
                                          expirationTranslator
                                              .translateExpiration(
                                                  tokenRule.getResponseSessionTokenDetails(),
                                                  ruleIndex,
                                                  ruleId,
                                                  tokenRule
                                                      .getTokenValueRule()
                                                      .getTokenValueProjection()
                                                      .getAttributeProjection()
                                                      .getAttributeKeyMatchCondition())
                                              .stream()
                                              .map(
                                                  action ->
                                                      AttributeRule.newBuilder()
                                                          .addInitialActions(action)
                                                          .build())
                                              .collect(Collectors.toUnmodifiableList())))))
                  .build(),
              tokenRule);
        }

        return predicateTranslator.addConditionalPredicateIfPresent(
            projectors.stream()
                .map(
                    projector ->
                        addAttributeAndProject(
                            sessionIdentificationConstants.buildKeyForNewSessionId(
                                ruleId, ruleIndex),
                            projector))
                .collect(Collectors.toUnmodifiableList()),
            tokenRule);
      case REQUEST_SESSION_TOKEN_DETAILS:
      default:
        // for custom projection, we don't know the case, request or response
        // so keeping the attribute name starting with traceableai.session<>
        return predicateTranslator.addConditionalPredicateIfPresent(
            projectors.stream()
                .map(
                    projector ->
                        addAttributeAndProject(
                            sessionIdentificationConstants.buildKeyForSessionId(ruleId, ruleIndex),
                            projector))
                .collect(Collectors.toUnmodifiableList()),
            tokenRule);
    }
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
