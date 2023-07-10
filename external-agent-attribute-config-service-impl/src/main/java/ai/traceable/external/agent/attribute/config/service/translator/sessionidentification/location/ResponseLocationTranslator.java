package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.ResponseAttributeKeyLocation;
import java.util.List;

public interface ResponseLocationTranslator {
  List<Projector> translateForResponse(MatchCondition matchCondition, AttributeRule attributeValue);

  ResponseAttributeKeyLocation getResponseAttributeKeyLocation();
}
