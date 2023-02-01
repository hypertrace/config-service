package ai.traceable.localprocessing.config.service.apinaming.http.utils;

import ai.traceable.platform.apientity.Segment;
import ai.traceable.platform.apientity.Wildcard;
import ai.traceable.platform.apientity.http.model.NodeType;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SegmentConverter {
  static final String REPLACEMENT_REGEX = "*";

  public ai.traceable.localprocessing.config.service.v1.Segment convertSegment(
      Segment segment, Map<NodeType, String> wildcardConfigMap) {
    if (segment.getName() == null) {
      log.error("Segment name is null: {}", segment);
    }
    if (isWildcard(segment)) {
      Wildcard wildcard = (Wildcard) segment.getName();
      String identificationRegex = wildcardConfigMap.get(getNodeType(wildcard));
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

  private NodeType getNodeType(Wildcard wildcard) {
    if (wildcard.getType() != null) {
      return NodeType.valueOf(wildcard.getType());
    } else if (wildcard.getWildcardType() != null) {
      return NodeType.valueOf(wildcard.getWildcardType().name());
    }
    log.error("Both type and wildcard type is not present in trie diff log mode {}", wildcard);
    // Putting it here as safety net. It shouldn't reach here ideally.
    return NodeType.HIGH_CARDINALITY;
  }
}
