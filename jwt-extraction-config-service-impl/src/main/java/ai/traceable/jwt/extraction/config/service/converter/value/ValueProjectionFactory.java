package ai.traceable.jwt.extraction.config.service.converter.value;

import ai.traceable.jwt.extraction.config.service.v1.JwtProcessingInstruction;
import java.util.ArrayList;
import java.util.List;

public class ValueProjectionFactory {

  public static List<ValueProjection> create(JwtProcessingInstruction instruction) {
    List<ValueProjection> valueProjections = new ArrayList<>();
    JwtProcessingInstruction.ValueExtraction valueExtraction = instruction.getValueExtraction();
    switch (valueExtraction.getSourceCase()) {
      case HEADER_KEY:
        valueProjections.add(new JwtHeaderProjection(valueExtraction.getHeaderKey()));
        break;
      case PAYLOAD_CLAIM_NAME:
        valueProjections.add(new JwtClaimProjection(valueExtraction.getPayloadClaimName()));
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported source case: " + valueExtraction.getSourceCase());
    }
    if (valueExtraction
        .getCaptureCase()
        .equals(JwtProcessingInstruction.ValueExtraction.CaptureCase.REGEX_CAPTURE_GROUP)) {
      valueProjections.add(new RegexCaptureGroupProjection(valueExtraction.getRegexCaptureGroup()));
    }
    return valueProjections;
  }
}
