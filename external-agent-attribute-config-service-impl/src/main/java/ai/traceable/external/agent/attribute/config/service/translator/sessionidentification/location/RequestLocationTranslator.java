package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.sessionidentification.config.service.v1.MatchCondition;
import ai.traceable.sessionidentification.config.service.v1.RequestAttributeKeyLocation;
import java.util.List;

public interface RequestLocationTranslator {
  List<Projector> translateForRequest(MatchCondition matchCondition, AttributeRule attributeValue);

  RequestAttributeKeyLocation getRequestAttributeKeyLocation();
}
