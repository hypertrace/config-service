package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction;

import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.instruction.InstructionTranslatorForJwtAction;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location.LocationTranslationState;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location.LocationTranslator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtLocation;
import ai.traceable.jwt.extraction.config.service.v1.JwtProcessingInstruction;
import ai.traceable.jwt.extraction.config.service.v1.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor(onConstructor_ = @Inject)
public class JwtExtractionRuleTranslator {
  private final PredicateTranslator predicateTranslator;
  private final LocationTranslator locationTranslator;
  private final Map<JwtProcessingInstruction.Action.ActionCase, InstructionTranslatorForJwtAction>
      instructionTranslatorForJwtActionMap;

  public Stream<AttributeRule> translateJwtExtractionRules(
      List<JwtExtractionRule> jwtExtractionRules) {
    return jwtExtractionRules.stream()
        .map(this::translateRule)
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  private Optional<AttributeRule> translateRule(JwtExtractionRule jwtExtractionRule) {
    // any invalid or unsupported operations will throw a runtime exception during translation
    // we catch the exception and drop that individual rule
    try {
      List<LocationTranslationState> locationTranslationStates = new ArrayList<>();
      for (JwtLocation location : jwtExtractionRule.getLocationsList()) {
        locationTranslationStates.addAll(locationTranslator.getLocationTranslationStates(location));
      }

      List<AttributeRule> extractionRules = new ArrayList<>();

      for (LocationTranslationState locationState : locationTranslationStates) {
        List<AttributeRule.Action> actionsAtLocation = new ArrayList<>();
        for (JwtProcessingInstruction instruction : jwtExtractionRule.getInstructionsList()) {
          InstructionTranslatorForJwtAction translatorForJwtAction =
              instructionTranslatorForJwtActionMap.get(instruction.getAction().getActionCase());
          if (translatorForJwtAction == null) {
            throw new JwtTranslationException(
                "Unsupported jwt action for translation "
                    + instruction.getAction().getActionCase());
          }
          actionsAtLocation.add(
              translatorForJwtAction.getAgentActionForLocationInstructionPair(
                  locationState, instruction));
        }
        extractionRules.add(
            locationTranslator.getCaptureAndExtractRuleForALocation(
                locationState,
                AttributeRule.newBuilder().addAllInitialActions(actionsAtLocation).build()));
      }

      AttributeRule mergedRules = mergeRulesIntoSingleOne(extractionRules);
      Optional<AttributeRule.Projector.ConditionalProjector.Predicate>
          topLevelConditionalPredicateOptional =
              jwtExtractionRule.hasPredicate()
                      && jwtExtractionRule.getPredicate().getPredicateCase()
                          != Predicate.PredicateCase.PREDICATE_NOT_SET
                  ? Optional.of(
                      predicateTranslator.translatePredicate(jwtExtractionRule.getPredicate()))
                  : Optional.empty();

      return topLevelConditionalPredicateOptional
          .map(
              topLevelConditionalPredicate ->
                  AttributeRule.newBuilder()
                      .setProjector(
                          AttributeRule.Projector.newBuilder()
                              .setConditionalProjector(
                                  AttributeRule.Projector.ConditionalProjector.newBuilder()
                                      .setPredicate(topLevelConditionalPredicate)
                                      .setAttributeRule(mergedRules)))
                      .build())
          .or(() -> Optional.of(mergedRules));

    } catch (JwtTranslationException e) {
      log.warn(
          "Failed to translate jwt extraction ruleId [{}], dropping it due to - {}",
          jwtExtractionRule.getId(),
          e.getMessage());
    }
    return Optional.empty();
  }

  private AttributeRule mergeRulesIntoSingleOne(List<AttributeRule> rules) {
    return AttributeRule.newBuilder()
        .setProjector(
            AttributeRule.Projector.newBuilder()
                .setEachMatchingProjector(
                    AttributeRule.Projector.EachMatchingProjector.newBuilder()
                        .addAllAttributeRules(rules)
                        .build()))
        .build();
  }
}
