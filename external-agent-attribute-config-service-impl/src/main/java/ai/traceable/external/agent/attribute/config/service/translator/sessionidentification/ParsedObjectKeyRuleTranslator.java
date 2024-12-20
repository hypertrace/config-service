package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector.ParsedObjectKeyRule;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.MatchOperator;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class ParsedObjectKeyRuleTranslator {
  private final MatchConditionTranslator matchConditionTranslator;

  public ParsedObjectKeyRule translate(
      MatchCondition matchCondition, AttributeRule attributeValue) {
    ParsedObjectKeyRule.Builder parsedObjectKeyRule = ParsedObjectKeyRule.newBuilder();
    if (matchCondition.getOperator().equals(MatchOperator.MATCH_OPERATOR_EQUALS)) {
      parsedObjectKeyRule.setKey(matchCondition.getMatchValue().getStringValue());
    } else {
      parsedObjectKeyRule.setKeyPredicate(matchConditionTranslator.translate(matchCondition));
    }
    return parsedObjectKeyRule.setAttributeRule(attributeValue).build();
  }
}
