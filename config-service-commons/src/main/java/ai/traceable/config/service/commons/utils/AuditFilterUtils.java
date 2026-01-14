package ai.traceable.config.service.commons.utils;

import ai.traceable.config.commons.v1.AuditFilter;
import lombok.experimental.UtilityClass;

@UtilityClass
public class AuditFilterUtils {
  public static boolean hasActiveAuditFilter(AuditFilter auditFilter) {
    if (AuditFilter.getDefaultInstance().equals(auditFilter)) {
      return false;
    }
    return auditFilter.hasCreatedRange()
        || auditFilter.hasUpdatedRange()
        || !auditFilter.getCreatedByContains().isEmpty()
        || !auditFilter.getLastUpdatedByUserContains().isEmpty();
  }
}
