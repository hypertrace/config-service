package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.instruction;

import static ai.traceable.external.agent.attribute.config.service.translator.AgentAttributeConstants.JWT_EXTRACTION_RULE_ATTRIBUTE_KEY_PREFIX;

import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.JwtTranslationException;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location.LocationTranslationState;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtProcessingInstruction;
import com.google.common.base.Joiner;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class AddAttributeActionTranslator implements InstructionTranslatorForJwtAction {
  private final ValueExtractionTranslator valueExtractionTranslator;

  @Override
  public AttributeRule.Action getAgentActionForLocationInstructionPair(
      LocationTranslationState locationTranslationResult, JwtProcessingInstruction instruction)
      throws JwtTranslationException {
    PartOfJwt partOfJwt = valueExtractionTranslator.getPartOfJwt(instruction.getValueExtraction());
    String attributeSuffix = instruction.getAction().getAddNewAttribute();
    String attributeKey =
        Joiner.on('.')
            .skipNulls()
            .join(
                JWT_EXTRACTION_RULE_ATTRIBUTE_KEY_PREFIX,
                partOfJwt.getValue(),
                locationTranslationResult.getFirstClassAttributeTag(),
                locationTranslationResult.getKeyUnderFirstClassAttribute().orElse(null),
                attributeSuffix);
    return AttributeRule.Action.newBuilder()
        .setAttributeAddition(
            AttributeRule.Action.AttributeAddition.newBuilder()
                .setAttributeKey(attributeKey)
                .setValueProjectionRule(
                    valueExtractionTranslator.getExtractionRule(instruction.getValueExtraction()))
                .build())
        .build();
  }
}
