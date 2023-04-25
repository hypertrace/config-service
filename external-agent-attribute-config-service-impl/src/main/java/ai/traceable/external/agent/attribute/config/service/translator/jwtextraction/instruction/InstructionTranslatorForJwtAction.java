package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.instruction;

import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.JwtTranslationException;
import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.location.LocationTranslationState;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtProcessingInstruction;

public interface InstructionTranslatorForJwtAction {
  AttributeRule.Action getAgentActionForLocationInstructionPair(
      LocationTranslationState locationTranslationResult, JwtProcessingInstruction instruction)
      throws JwtTranslationException;
}
