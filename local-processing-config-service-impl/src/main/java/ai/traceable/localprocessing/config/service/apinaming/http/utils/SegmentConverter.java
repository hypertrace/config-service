package ai.traceable.localprocessing.config.service.apinaming.http.utils;

import ai.traceable.platform.apientity.Segment;
import ai.traceable.platform.apientity.TrieNodeType;
import ai.traceable.platform.apientity.Wildcard;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SegmentConverter {
  static final String REPLACEMENT_REGEX = "*";

  public ai.traceable.localprocessing.config.service.v1.Segment convertSegment(
      Segment segment, Map<TrieNodeType, String> wildcardConfigMap) {
    if (segment.getName() == null) {
      log.error("Segment name is null: {}", segment);
    }
    if (isWildcard(segment)) {
      Wildcard wildcard = (Wildcard) segment.getName();
      String identificationRegex = wildcardConfigMap.get(wildcard.getWildcardType());
      var replacementPattern = REPLACEMENT_REGEX;
      if (!wildcard.getExtension().isEmpty()) {
        identificationRegex = identificationRegex + "." + wildcard.getExtension();
        replacementPattern = REPLACEMENT_REGEX + "." + wildcard.getExtension();
      }
      return ai.traceable.localprocessing.config.service.v1.Segment.newBuilder()
          .setWildcard(
              ai.traceable.localprocessing.config.service.v1.Wildcard.newBuilder()
                  .setIdentificationRegex(identificationRegex)
                  .setReplacementPattern(replacementPattern)
                  .build())
          .build();
    }
    return ai.traceable.localprocessing.config.service.v1.Segment.newBuilder()
        .setName(segment.getName().toString())
        .build();
  }

  private boolean isWildcard(Segment segment) {
    return Wildcard.class.isAssignableFrom(segment.getName().getClass());
  }
}
