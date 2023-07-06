package ai.traceable.risk.config.service.v2.elements.normalizer;

import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class LabelIdNormalizer {

  List<String> SYSTEM_LABEL_IDS =
      List.of("External", "Internal", "Sensitive", "Sentry", "Critical");

  public String normalizeId(String labelId) {
    if (isSystemLabel(labelId)) {
      return StringUtils.capitalize(labelId);
    }
    return labelId;
  }

  private boolean isSystemLabel(String labelId) {
    return SYSTEM_LABEL_IDS.stream()
        .anyMatch(systemLabelId -> systemLabelId.equalsIgnoreCase(labelId));
  }
}
