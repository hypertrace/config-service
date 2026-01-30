package ai.traceable.detection.exclusion.config.service.v1.rules;

import static ai.traceable.config.service.commons.utils.AuditFilterUtils.getVisibleUserEmail;

import ai.traceable.config.commons.v1.AuditDetails;
import ai.traceable.config.commons.v1.AuditFilter;
import ai.traceable.config.commons.v1.CreationDetails;
import ai.traceable.config.commons.v1.LastUpdateDetails;
import ai.traceable.config.commons.v1.TimestampRange;
import ai.traceable.config.utils.TimestampConverter;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleRecord;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import com.google.inject.Inject;
import java.time.Instant;
import org.hypertrace.config.objectstore.ContextualConfigObject;

public class DetectionExclusionAuditHelper {

  private final TimestampConverter timestampConverter;
  private final DetectionExclusionConfigServiceConfig detectionExclusionConfigServiceConfig;

  @Inject
  public DetectionExclusionAuditHelper(
      TimestampConverter timestampConverter,
      DetectionExclusionConfigServiceConfig detectionExclusionConfigServiceConfig) {
    this.timestampConverter = timestampConverter;
    this.detectionExclusionConfigServiceConfig = detectionExclusionConfigServiceConfig;
  }

  public DetectionExclusionRuleRecord toRuleRecord(
      ContextualConfigObject<DetectionExclusionRule> contextual) {
    return DetectionExclusionRuleRecord.newBuilder()
        .setRule(contextual.getData())
        .setAuditDetails(buildAuditDetails(contextual))
        .build();
  }

  public boolean matchesAuditFilters(
      ContextualConfigObject<DetectionExclusionRule> configObject, GetRulesFilter filter) {
    if (!filter.hasAuditFilter()) {
      return true;
    }
    AuditFilter auditFilter = filter.getAuditFilter();
    return matchesCreatedRange(configObject, auditFilter)
        && matchesUpdatedRange(configObject, auditFilter)
        && matchesCreatedByContains(configObject, auditFilter)
        && matchesLastUpdatedByContains(configObject, auditFilter);
  }

  private AuditDetails buildAuditDetails(ContextualConfigObject<?> contextual) {
    AuditDetails.Builder builder = AuditDetails.newBuilder();

    Instant creationTimestamp = contextual.getCreationTimestamp();
    if (creationTimestamp != null && creationTimestamp.getEpochSecond() > 0) {
      CreationDetails.Builder creationBuilder =
          CreationDetails.newBuilder().setCreatedAt(timestampConverter.convert(creationTimestamp));
      if (contextual.getCreatedByEmail() != null) {
        creationBuilder.setCreatedBy(contextual.getCreatedByEmail());
      }
      builder.setCreationDetails(creationBuilder.build());
    }

    Instant lastUserUpdateTimestamp = contextual.getLastUserUpdateTimestamp();
    if (lastUserUpdateTimestamp != null && lastUserUpdateTimestamp.getEpochSecond() > 0) {
      LastUpdateDetails.Builder updateBuilder =
          LastUpdateDetails.newBuilder()
              .setUpdatedAt(timestampConverter.convert(lastUserUpdateTimestamp));
      String userEmail =
          getVisibleUserEmail(
              contextual.getLastUserUpdateEmail(),
              contextual.getLastUpdateEmail(),
              this.detectionExclusionConfigServiceConfig.getUserVisibleEmailConfig());
      updateBuilder.setUpdatedBy(userEmail);
      builder.setLastUserUpdateDetails(updateBuilder.build());
    }

    return builder.build();
  }

  private boolean matchesCreatedRange(
      ContextualConfigObject<DetectionExclusionRule> configObject, AuditFilter auditFilter) {
    if (!auditFilter.hasCreatedRange()) {
      return true;
    }
    Instant creationTime = configObject.getCreationTimestamp();
    if (creationTime == null || creationTime.getEpochSecond() == 0) {
      return false;
    }
    return isTimestampInRange(creationTime, auditFilter.getCreatedRange());
  }

  private boolean matchesUpdatedRange(
      ContextualConfigObject<DetectionExclusionRule> configObject, AuditFilter auditFilter) {
    if (!auditFilter.hasUpdatedRange()) {
      return true;
    }
    Instant lastUpdateTime = configObject.getLastUserUpdateTimestamp();
    if (lastUpdateTime == null || lastUpdateTime.getEpochSecond() == 0) {
      return false;
    }
    return isTimestampInRange(lastUpdateTime, auditFilter.getUpdatedRange());
  }

  private boolean matchesCreatedByContains(
      ContextualConfigObject<DetectionExclusionRule> configObject, AuditFilter auditFilter) {
    String createdByContains = auditFilter.getCreatedByContains();
    if (createdByContains.isEmpty()) {
      return true;
    }
    String createdBy = configObject.getCreatedByEmail();
    if (createdBy == null || createdBy.isEmpty()) {
      return false;
    }
    return containsIgnoreCase(createdBy, createdByContains);
  }

  private boolean matchesLastUpdatedByContains(
      ContextualConfigObject<DetectionExclusionRule> configObject, AuditFilter auditFilter) {
    String lastUpdatedByContains = auditFilter.getLastUpdatedByUserContains();
    if (lastUpdatedByContains.isEmpty()) {
      return true;
    }
    String lastUserUpdateEmail =
        getVisibleUserEmail(
            configObject.getLastUserUpdateEmail(),
            configObject.getLastUpdateEmail(),
            this.detectionExclusionConfigServiceConfig.getUserVisibleEmailConfig());
    return lastUserUpdateEmail != null
        && lastUserUpdateEmail.toLowerCase().contains(lastUpdatedByContains.toLowerCase());
  }

  private boolean isTimestampInRange(Instant timestamp, TimestampRange range) {
    if (range.hasStart()) {
      Instant startTime = timestampConverter.convertToInstant(range.getStart());
      if (timestamp.isBefore(startTime)) {
        return false;
      }
    }
    if (range.hasEnd()) {
      Instant endTime = timestampConverter.convertToInstant(range.getEnd());
      return !timestamp.isAfter(endTime);
    }
    return true;
  }

  private boolean containsIgnoreCase(String source, String searchTerm) {
    return source.toLowerCase().contains(searchTerm.toLowerCase());
  }
}
