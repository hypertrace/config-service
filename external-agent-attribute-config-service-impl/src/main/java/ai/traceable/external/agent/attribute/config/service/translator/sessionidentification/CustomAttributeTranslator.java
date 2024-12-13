package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.translator.AttributeRuleBuilder;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.sessionidentification.config.service.v1.AttributeProjection;
import ai.traceable.sessionidentification.config.service.v1.CustomAttributeRule;
import ai.traceable.sessionidentification.config.service.v1.RequestSessionTokenDetails;
import ai.traceable.sessionidentification.config.service.v1.ResponseSessionTokenDetails;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class CustomAttributeTranslator {
  private final ProjectionRootTranslator projectionRootTranslator;
  private final AttributeRuleBuilder attributeRuleBuilder;
  private final SessionIdentificationConstants sessionIdentificationConstants;

  List<AttributeRule> translateCustomAttribute(
      ResponseSessionTokenDetails responseSessionTokenDetails,
      int ruleIndex,
      String ruleId,
      AttributeProjection attributeProjection) {
    String sessionIdAttr =
        sessionIdentificationConstants.buildKeyForNewSessionId(ruleId, ruleIndex);
    Map<CustomAttributeRule.JwtAttributeExtraction, List<AttributeRule.Projector>>
        customAttrProjectionMap =
            projectionRootTranslator.translateForCustomAttribute(
                responseSessionTokenDetails, attributeProjection);
    return customAttrProjectionMap.entrySet().stream()
        .map(
            entry ->
                addCustomAttributeAndProject(
                    entry.getValue(),
                    sessionIdentificationConstants.buildKeyForJwtAttrValue(
                        ruleId, ruleIndex, entry.getKey()),
                    sessionIdAttr))
        .collect(Collectors.toUnmodifiableList());
  }

  List<AttributeRule> translateCustomAttribute(
      RequestSessionTokenDetails requestSessionTokenDetails,
      int ruleIndex,
      String ruleId,
      AttributeProjection attributeProjection) {
    String sessionIdAttr = sessionIdentificationConstants.buildKeyForSessionId(ruleId, ruleIndex);
    Map<CustomAttributeRule.JwtAttributeExtraction, List<AttributeRule.Projector>>
        customAttrProjectionMap =
            projectionRootTranslator.translateForCustomAttribute(
                requestSessionTokenDetails, attributeProjection);
    return customAttrProjectionMap.entrySet().stream()
        .map(
            entry ->
                addCustomAttributeAndProject(
                    entry.getValue(),
                    sessionIdentificationConstants.buildKeyForJwtAttrValue(
                        ruleId, ruleIndex, entry.getKey()),
                    sessionIdAttr))
        .collect(Collectors.toUnmodifiableList());
  }

  private AttributeRule addCustomAttributeAndProject(
      List<AttributeRule.Projector> projectors, String customAttrKey, String sessionIdAttr) {
    return attributeRuleBuilder.buildFirstMatchingProjectorAttributeRule(
        projectors.stream()
            .map(
                projector ->
                    AttributeRule.newBuilder()
                        .setProjector(
                            AttributeRule.Projector.newBuilder()
                                .setConditionalProjector(
                                    AttributeRule.Projector.ConditionalProjector.newBuilder()
                                        .setPredicate(
                                            AttributeRule.Projector.ConditionalProjector.Predicate
                                                .newBuilder()
                                                .setAttributePredicate(
                                                    AttributeRule.Projector.ConditionalProjector
                                                        .Predicate.AttributePredicate.newBuilder()
                                                        .setNamePredicate(
                                                            AttributeRule.Projector
                                                                .ConditionalProjector.Predicate
                                                                .StringPredicate.newBuilder()
                                                                .setOperator(
                                                                    AttributeRule.Projector
                                                                        .ConditionalProjector
                                                                        .Predicate
                                                                        .ComparisonOperator
                                                                        .COMPARISON_OPERATOR_EQUALS)
                                                                .setValue(sessionIdAttr))))
                                        .setAttributeRule(
                                            AttributeRule.newBuilder().setProjector(projector))))
                        .build())
            .collect(Collectors.toUnmodifiableList()),
        customAttrKey);
  }
}
