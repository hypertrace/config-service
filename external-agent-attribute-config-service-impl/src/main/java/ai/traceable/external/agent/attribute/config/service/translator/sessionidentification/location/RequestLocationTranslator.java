package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.RequestAttributeKeyLocation;
import ai.traceable.sessionidentification.config.service.v1.RuleCreationSource;
import java.util.List;

public interface RequestLocationTranslator {
  List<Projector> translateForRequest(MatchCondition matchCondition, AttributeRule attributeValue);

  RequestAttributeKeyLocation getRequestAttributeKeyLocation();

  default List<Projector> translateForRequest(
      MatchCondition matchCondition,
      AttributeRule attributeValue,
      RuleCreationSource ruleCreationSource) {
    return this.translateForRequest(matchCondition, attributeValue);
  }
}
