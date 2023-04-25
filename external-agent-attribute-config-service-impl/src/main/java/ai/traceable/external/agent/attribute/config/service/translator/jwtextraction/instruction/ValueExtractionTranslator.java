package ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.instruction;

import ai.traceable.external.agent.attribute.config.service.translator.jwtextraction.JwtTranslationException;
import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.jwt.extraction.config.service.v1.JwtProcessingInstruction;

class ValueExtractionTranslator {
  private static final AttributeRule NOOP_PROJECTOR =
      AttributeRule.newBuilder()
          .setProjector(
              AttributeRule.Projector.newBuilder()
                  .setNoOpProjector(AttributeRule.Projector.NoOpProjector.newBuilder().build()))
          .build();

  PartOfJwt getPartOfJwt(JwtProcessingInstruction.ValueExtraction valueExtraction)
      throws JwtTranslationException {
    switch (valueExtraction.getSourceCase()) {
      case PAYLOAD_CLAIM_NAME:
        return PartOfJwt.PAYLOAD;
      case HEADER_KEY:
        return PartOfJwt.HEADER;
      default:
        throw new JwtTranslationException(
            "Unknown value extraction source case " + valueExtraction.getSourceCase());
    }
  }

  AttributeRule getExtractionRule(JwtProcessingInstruction.ValueExtraction valueExtraction)
      throws JwtTranslationException {
    return AttributeRule.newBuilder()
        .setProjector(
            AttributeRule.Projector.newBuilder().setJwtProjector(getJwtProjector(valueExtraction)))
        .build();
  }

  private AttributeRule.Projector.JwtProjector getJwtProjector(
      JwtProcessingInstruction.ValueExtraction valueExtraction) throws JwtTranslationException {
    AttributeRule valueCaptureRule = getValueCaptureRule(valueExtraction);
    switch (valueExtraction.getSourceCase()) {
      case PAYLOAD_CLAIM_NAME:
        return AttributeRule.Projector.JwtProjector.newBuilder()
            .setClaimRule(
                AttributeRule.Projector.ParsedObjectKeyRule.newBuilder()
                    .setKey(valueExtraction.getPayloadClaimName())
                    .setAttributeRule(valueCaptureRule))
            .build();
      case HEADER_KEY:
        return AttributeRule.Projector.JwtProjector.newBuilder()
            .setHeaderRule(
                AttributeRule.Projector.ParsedObjectKeyRule.newBuilder()
                    .setKey(valueExtraction.getHeaderKey())
                    .setAttributeRule(valueCaptureRule))
            .build();
      default:
        throw new JwtTranslationException(
            "Unknown value extraction source case " + valueExtraction.getSourceCase());
    }
  }

  private static AttributeRule getValueCaptureRule(
      JwtProcessingInstruction.ValueExtraction valueExtraction) throws JwtTranslationException {
    switch (valueExtraction.getCaptureCase()) {
      case RAW_VALUE:
        return NOOP_PROJECTOR;
      case REGEX_CAPTURE_GROUP:
        return AttributeRule.newBuilder()
            .setProjector(
                AttributeRule.Projector.newBuilder()
                    .setRegexCaptureGroupProjector(
                        AttributeRule.Projector.RegexCaptureGroupProjector.newBuilder()
                            .setRegexCaptureGroup(valueExtraction.getRegexCaptureGroup())))
            .build();
      default:
        throw new JwtTranslationException(
            "Unsupported value extraction capture case " + valueExtraction.getCaptureCase());
    }
  }
}
