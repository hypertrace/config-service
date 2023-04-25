package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location;

import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.JwtTranslationException;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtLocation;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class LocationTranslator {
  private final Map<JwtLocation.LocationCase, AbstractLocationTranslator>
      locationCaseTranslatorsMap;

  public List<LocationTranslationState> getLocationTranslationStates(JwtLocation jwtLocation)
      throws JwtTranslationException {
    AbstractLocationTranslator locationCaseTranslator =
        locationCaseTranslatorsMap.get(jwtLocation.getLocationCase());
    if (locationCaseTranslator == null) {
      throw new JwtTranslationException(
          "Unsupported location case " + jwtLocation.getLocationCase());
    }
    return locationCaseTranslator.translateCase(jwtLocation);
  }

  public AttributeRule getCaptureAndExtractRuleForALocation(
      LocationTranslationState locationTranslationResult,
      AttributeRule rawRuleAtLocationWithActions) {
    AttributeRule transformedRule =
        locationTranslationResult
            .getLocationCaptureRuleTransformation()
            .orElse(Function.identity())
            .apply(addRegexCaptureGroup(locationTranslationResult, rawRuleAtLocationWithActions));
    return AttributeRule.newBuilder()
        .setProjector(
            AttributeRule.Projector.newBuilder()
                .setAttributeProjector(
                    AttributeRule.Projector.AttributeProjector.newBuilder()
                        .setAttributeKey(locationTranslationResult.getFirstClassAttributeTag())
                        .setAttributeRule(transformedRule)
                        .build())
                .build())
        .build();
  }

  private AttributeRule addRegexCaptureGroup(
      LocationTranslationState locationTranslationResult,
      AttributeRule rawRuleAtLocationWithActions) {
    return locationTranslationResult
        .getRegexCaptureGroup()
        .map(
            regex ->
                AttributeRule.newBuilder()
                    .setProjector(
                        AttributeRule.Projector.newBuilder()
                            .setRegexCaptureGroupProjector(
                                AttributeRule.Projector.RegexCaptureGroupProjector.newBuilder()
                                    .setRegexCaptureGroup(regex)
                                    .setAttributeRule(rawRuleAtLocationWithActions)
                                    .build())
                            .build())
                    .build())
        .orElse(rawRuleAtLocationWithActions);
  }
}
