package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location;

import ai.traceable.external.agent.attribute.config.service.translator.TestUtils;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.JwtExtractionTranslationModule;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.instruction.AddAttributeActionTranslator;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtExtractionRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtProcessingInstruction;
import com.google.inject.Guice;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;

public class JwtLocationTestUtils {
  private static final AddAttributeActionTranslator addAttributeActionTranslator =
      Guice.createInjector(new JwtExtractionTranslationModule())
          .getInstance(AddAttributeActionTranslator.class);
  private static final List<JwtProcessingInstruction> INSTRUCTION_LIST =
      List.of(
          JwtProcessingInstruction.newBuilder()
              .setAction(
                  JwtProcessingInstruction.Action.newBuilder().setAddNewAttribute("exp").build())
              .setValueExtraction(
                  JwtProcessingInstruction.ValueExtraction.newBuilder()
                      .setPayloadClaimName("exp")
                      .setRawValue(
                          JwtProcessingInstruction.ValueExtraction.RawValue.getDefaultInstance())
                      .build())
              .build(),
          JwtProcessingInstruction.newBuilder()
              .setAction(
                  JwtProcessingInstruction.Action.newBuilder().setAddNewAttribute("alg").build())
              .setValueExtraction(
                  JwtProcessingInstruction.ValueExtraction.newBuilder()
                      .setHeaderKey("alg")
                      .setRawValue(
                          JwtProcessingInstruction.ValueExtraction.RawValue.getDefaultInstance())
                      .build())
              .build());

  @SneakyThrows
  static void assertAttributeRuleWithActions(
      AbstractLocationTranslator locationTranslator, String inputFile, String outputFile) {
    JwtExtractionRule rule = TestUtils.getJwtExtractionRule(inputFile);
    List<LocationTranslationState> locationTranslationStates =
        rule.getLocationsList().stream()
            .map(
                jwtLocation -> {
                  try {
                    return locationTranslator.translateCase(jwtLocation);
                  } catch (Exception e) {
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .collect(Collectors.toUnmodifiableList());
    List<AttributeRule> attributeRulesWithActions = new ArrayList<>();
    for (LocationTranslationState locationState : locationTranslationStates) {
      List<AttributeRule.Action> actionsAtLocation = new ArrayList<>();
      for (JwtProcessingInstruction instruction : INSTRUCTION_LIST) {
        actionsAtLocation.add(
            addAttributeActionTranslator.getAgentActionForLocationInstructionPair(
                locationState, instruction));
      }
      attributeRulesWithActions.add(
          locationState
              .getLocationCaptureRuleTransformation()
              .orElse(Function.identity())
              .apply(AttributeRule.newBuilder().addAllInitialActions(actionsAtLocation).build()));
    }
    Assertions.assertEquals(
        TestUtils.getExpectedAttributeRules(outputFile), attributeRulesWithActions);
  }
}
