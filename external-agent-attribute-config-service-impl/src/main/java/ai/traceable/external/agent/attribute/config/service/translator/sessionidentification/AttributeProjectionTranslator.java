package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location.RequestLocationTranslatorLookup;
import ai.traceable.external.agent.attribute.config.service.translator.sessionidentification.location.ResponseLocationTranslatorLookup;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.sessionidentification.config.service.v1.ProjectionRoot;
import ai.traceable.sessionidentification.config.service.v1.RuleCreationSource;
import ai.traceable.sessionidentification.config.service.v1.SessionTokenRule;
import jakarta.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class AttributeProjectionTranslator {
  private final ValueProjectionsTranslator valueProjectionsTranslator;
  private final ResponseLocationTranslatorLookup responseLocationTranslatorLookup;
  private final RequestLocationTranslatorLookup requestLocationTranslatorLookup;

  List<Projector> translateForRequest(
      SessionTokenRule tokenRule, RuleCreationSource ruleCreationSource) {
    ProjectionRoot projectionRoot = tokenRule.getTokenValueRule().getTokenValueProjection();
    AttributeRule valueProjection =
        valueProjectionsTranslator.translateValueProjections(
            projectionRoot.getAttributeProjection().getValueProjectionsInOrderList(),
            AttributeRule.newBuilder());
    return requestLocationTranslatorLookup
        .getTranslator(tokenRule.getRequestSessionTokenDetails().getTokenLocation())
        .translateForRequest(
            projectionRoot.getAttributeProjection().getAttributeKeyMatchCondition(),
            valueProjection,
            ruleCreationSource);
  }

  public List<Projector> translateForResponse(SessionTokenRule tokenRule) {
    ProjectionRoot projectionRoot = tokenRule.getTokenValueRule().getTokenValueProjection();
    return responseLocationTranslatorLookup
        .getTranslator(tokenRule.getResponseSessionTokenDetails().getTokenLocation())
        .translateForResponse(
            projectionRoot.getAttributeProjection().getAttributeKeyMatchCondition(),
            valueProjectionsTranslator.translateValueProjections(
                projectionRoot.getAttributeProjection().getValueProjectionsInOrderList(),
                AttributeRule.newBuilder()));
  }
}
