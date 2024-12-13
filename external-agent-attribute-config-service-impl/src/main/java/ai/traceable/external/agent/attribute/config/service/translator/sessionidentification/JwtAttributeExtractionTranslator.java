package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule;
import ai.traceable.sessionidentification.config.service.v1.CustomAttributeRule.JwtAttributeExtraction;
import javax.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class JwtAttributeExtractionTranslator {
  private static final AttributeRule NOOP_PROJECTOR =
      AttributeRule.newBuilder()
          .setProjector(
              AttributeRule.Projector.newBuilder()
                  .setNoOpProjector(AttributeRule.Projector.NoOpProjector.newBuilder().build()))
          .build();

  AttributeRule.Builder getExtractionRule(JwtAttributeExtraction jwtAttributeExtraction)
      throws Exception {
    return AttributeRule.newBuilder()
        .setProjector(
            AttributeRule.Projector.newBuilder()
                .setJwtProjector(getJwtProjector(jwtAttributeExtraction)));
  }

  private AttributeRule.Projector.JwtProjector getJwtProjector(
      JwtAttributeExtraction jwtAttributeExtraction) throws Exception {
    AttributeRule valueCaptureRule = getValueCaptureRule(jwtAttributeExtraction);
    switch (jwtAttributeExtraction.getSourceCase()) {
      case PAYLOAD_CLAIM_NAME:
        return AttributeRule.Projector.JwtProjector.newBuilder()
            .setClaimRule(
                AttributeRule.Projector.ParsedObjectKeyRule.newBuilder()
                    .setKey(jwtAttributeExtraction.getPayloadClaimName())
                    .setAttributeRule(valueCaptureRule))
            .build();
      case HEADER_KEY:
        return AttributeRule.Projector.JwtProjector.newBuilder()
            .setHeaderRule(
                AttributeRule.Projector.ParsedObjectKeyRule.newBuilder()
                    .setKey(jwtAttributeExtraction.getHeaderKey())
                    .setAttributeRule(valueCaptureRule))
            .build();
      default:
        throw new RuntimeException(
            "Unknown value extraction source case " + jwtAttributeExtraction.getSourceCase());
    }
  }

  private static AttributeRule getValueCaptureRule(JwtAttributeExtraction jwtAttributeExtraction)
      throws Exception {
    switch (jwtAttributeExtraction.getCaptureCase()) {
      case RAW_VALUE:
        return NOOP_PROJECTOR;
      case REGEX_CAPTURE_GROUP:
        return AttributeRule.newBuilder()
            .setProjector(
                AttributeRule.Projector.newBuilder()
                    .setRegexCaptureGroupProjector(
                        AttributeRule.Projector.RegexCaptureGroupProjector.newBuilder()
                            .setRegexCaptureGroup(jwtAttributeExtraction.getRegexCaptureGroup())))
            .build();
      default:
        throw new RuntimeException(
            "Unsupported value extraction capture case " + jwtAttributeExtraction.getCaptureCase());
    }
  }
}
