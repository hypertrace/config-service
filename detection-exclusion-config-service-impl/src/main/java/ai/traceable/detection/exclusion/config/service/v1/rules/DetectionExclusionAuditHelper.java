package ai.traceable.detection.exclusion.config.service.v1.rules;

import static ai.traceable.audit.utils.AuditDetailsBuilder.buildAuditDetails;

import ai.traceable.audit.utils.AuditFilterUtils;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionConfigServiceConfig;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleRecord;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import com.google.inject.Inject;
import org.hypertrace.config.objectstore.ContextualConfigObject;

public class DetectionExclusionAuditHelper {

  private final DetectionExclusionConfigServiceConfig detectionExclusionConfigServiceConfig;

  @Inject
  public DetectionExclusionAuditHelper(
      DetectionExclusionConfigServiceConfig detectionExclusionConfigServiceConfig) {
    this.detectionExclusionConfigServiceConfig = detectionExclusionConfigServiceConfig;
  }

  public DetectionExclusionRuleRecord toRuleRecord(
      ContextualConfigObject<DetectionExclusionRule> contextual) {
    return DetectionExclusionRuleRecord.newBuilder()
        .setRule(contextual.getData())
        .setAuditDetails(
            buildAuditDetails(
                contextual, detectionExclusionConfigServiceConfig.getUserVisibleEmailConfig()))
        .build();
  }

  public boolean matchesAuditFilters(
      ContextualConfigObject<DetectionExclusionRule> configObject, GetRulesFilter filter) {
    return AuditFilterUtils.matchesAuditFilters(
        configObject,
        filter.getAuditFilter(),
        detectionExclusionConfigServiceConfig.getUserVisibleEmailConfig());
  }
}
