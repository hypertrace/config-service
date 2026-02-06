package ai.traceable.audit.utils;

import ai.traceable.config.commons.v1.AuditFilter;
import ai.traceable.config.commons.v1.TimestampRange;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.config.objectstore.ContextualConfigObject;

@UtilityClass
public class AuditFilterUtils {

  static Pair<Instant, String> getLastUserUpdateDetails(
      ContextualConfigObject<?> contextual, UserVisibleEmailConfig config) {
    if (contextual.getLastUserUpdateTimestamp() != null
        && contextual.getLastUserUpdateTimestamp().getEpochSecond() > 0) {
      return Pair.of(
          contextual.getLastUserUpdateTimestamp(),
          config.maskEmailIfNotVisible(contextual.getLastUserUpdateEmail()));
    }
    return Pair.of(
        contextual.getLastUpdatedTimestamp(),
        config.maskEmailIfNotVisible(contextual.getLastUpdateEmail()));
  }

  public static boolean hasActiveAuditFilter(AuditFilter auditFilter) {
    if (AuditFilter.getDefaultInstance().equals(auditFilter)) {
      return false;
    }
    return auditFilter.hasCreatedRange()
        || auditFilter.hasUpdatedRange()
        || !auditFilter.getCreatedByContains().isEmpty()
        || !auditFilter.getLastUpdatedByUserContains().isEmpty();
  }

  public static boolean matchesAuditFilters(
      ContextualConfigObject<?> configObject,
      AuditFilter auditFilter,
      UserVisibleEmailConfig config) {
    if (!hasActiveAuditFilter(auditFilter)) {
      return true;
    }
    return matchesCreatedByFilters(configObject, auditFilter)
        && matchesLastUpdatedByFilters(configObject, auditFilter, config);
  }

  private static boolean matchesCreatedByFilters(
      ContextualConfigObject<?> configObject, AuditFilter auditFilter) {
    return matchesCreatedRange(configObject.getCreationTimestamp(), auditFilter)
        && matchesCreatedByContains(configObject.getCreatedByEmail(), auditFilter);
  }

  private static boolean matchesLastUpdatedByFilters(
      ContextualConfigObject<?> configObject,
      AuditFilter auditFilter,
      UserVisibleEmailConfig config) {
    Pair<Instant, String> lastUserUpdateDetails = getLastUserUpdateDetails(configObject, config);
    return matchesUpdatedRange(lastUserUpdateDetails.getLeft(), auditFilter)
        && matchesLastUpdatedByContains(lastUserUpdateDetails.getRight(), auditFilter);
  }

  private boolean matchesCreatedRange(Instant creationTime, AuditFilter auditFilter) {
    if (!auditFilter.hasCreatedRange()) {
      return true;
    }
    if (creationTime == null || creationTime.getEpochSecond() == 0) {
      return false;
    }
    return isTimestampInRange(creationTime, auditFilter.getCreatedRange());
  }

  private boolean matchesUpdatedRange(Instant lastUpdateTime, AuditFilter auditFilter) {
    if (!auditFilter.hasUpdatedRange()) {
      return true;
    }
    if (lastUpdateTime == null || lastUpdateTime.getEpochSecond() == 0) {
      return false;
    }
    return isTimestampInRange(lastUpdateTime, auditFilter.getUpdatedRange());
  }

  private boolean matchesCreatedByContains(String createdByEmail, AuditFilter auditFilter) {
    String createdByContains = auditFilter.getCreatedByContains();
    if (createdByContains.isEmpty()) {
      return true;
    }
    if (createdByEmail == null || createdByEmail.isEmpty()) {
      return false;
    }
    return containsIgnoreCase(createdByEmail, createdByContains);
  }

  private boolean matchesLastUpdatedByContains(
      String lastUserUpdateEmail, AuditFilter auditFilter) {
    String lastUpdatedByContains = auditFilter.getLastUpdatedByUserContains();
    if (lastUpdatedByContains.isEmpty()) {
      return true;
    }
    return lastUserUpdateEmail != null
        && lastUserUpdateEmail.toLowerCase().contains(lastUpdatedByContains.toLowerCase());
  }

  private boolean isTimestampInRange(Instant timestamp, TimestampRange range) {
    if (range.hasStart()) {
      Instant startTime = convertToInstant(range.getStart());
      if (timestamp.isBefore(startTime)) {
        return false;
      }
    }
    if (range.hasEnd()) {
      Instant endTime = convertToInstant(range.getEnd());
      return !timestamp.isAfter(endTime);
    }
    return true;
  }

  private boolean containsIgnoreCase(String source, String searchTerm) {
    return source.toLowerCase().contains(searchTerm.toLowerCase());
  }

  private Instant convertToInstant(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }
}
