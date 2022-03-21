package ai.traceable.localprocessing.config.service.apinaming.http.utils;

import ai.traceable.localprocessing.config.service.v1.WildcardType;
import ai.traceable.platform.apientity.Segment;
import ai.traceable.platform.apientity.TrieNodeType;
import ai.traceable.platform.apientity.Wildcard;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SegmentConverter {

  public ai.traceable.localprocessing.config.service.v1.Value convertSegmentToValue(
      Segment segment) {
    if (segment.getName() == null) {
      log.error("Segment name is null: {}", segment);
    }
    if (isWildcard(segment)) {
      return ai.traceable.localprocessing.config.service.v1.Value.newBuilder()
          .setWildcard(convertWildcard((Wildcard) segment.getName()))
          .build();
    }
    return ai.traceable.localprocessing.config.service.v1.Value.newBuilder()
        .setName(segment.getName().toString())
        .build();
  }

  private boolean isWildcard(Segment segment) {
    return Wildcard.class.isAssignableFrom(segment.getName().getClass());
  }

  private ai.traceable.localprocessing.config.service.v1.Wildcard convertWildcard(
      Wildcard wildcard) {
    ai.traceable.localprocessing.config.service.v1.Wildcard.Builder wildcardBuilder =
        ai.traceable.localprocessing.config.service.v1.Wildcard.newBuilder()
            .setWildcardType(convertWildcardType(wildcard.getWildcardType()));
    if (!wildcard.getExtension().isEmpty()) {
      wildcardBuilder = wildcardBuilder.setExtension(wildcard.getExtension());
    }
    return wildcardBuilder.build();
  }

  private WildcardType convertWildcardType(TrieNodeType wildcardType) {
    switch (wildcardType) {
      case ID:
        return WildcardType.WILDCARD_TYPE_ID;
      case LOW_CARDINALITY:
        return WildcardType.WILDCARD_TYPE_LOW_CARDINALITY;
      case HIGH_CARDINALITY:
        return WildcardType.WILDCARD_TYPE_HIGH_CARDINALITY;
      case MEDIUM_CARDINALITY:
        return WildcardType.WILDCARD_TYPE_MEDIUM_CARDINALITY;
      case WHITELIST:
        return WildcardType.WILDCARD_TYPE_UNSPECIFIED;
      default:
        log.error("Unknown wildcard type:{}", wildcardType);
        return WildcardType.WILDCARD_TYPE_UNSPECIFIED;
    }
  }
}
