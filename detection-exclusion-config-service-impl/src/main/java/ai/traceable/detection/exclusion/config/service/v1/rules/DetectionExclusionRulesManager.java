package ai.traceable.detection.exclusion.config.service.v1.rules;

import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_ALERT;
import static ai.traceable.platform.utils.ip.IpAddressParsingUtils.parseRawIpRange;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.IpAddressCondition;
import ai.traceable.detection.exclusion.config.service.v1.rules.migration.RulesMigrationManager;
import ai.traceable.platform.utils.ip.IpAddressParsingUtils;
import io.grpc.Status;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DetectionExclusionRulesManager implements RulesManager {
  private final DetectionExclusionRulesStore rulesStore;
  private final UuidGenerator uuidGenerator;
  private final RulesMigrationManager rulesMigrationManager;
  private final Clock clock;

  @Inject
  public DetectionExclusionRulesManager(
      DetectionExclusionRulesStore rulesStore,
      UuidGenerator uuidGenerator,
      RulesMigrationManager rulesMigrationManager,
      Clock clock) {
    this.rulesStore = rulesStore;
    this.uuidGenerator = uuidGenerator;
    this.rulesMigrationManager = rulesMigrationManager;
    this.clock = clock;
  }

  @Override
  public List<DetectionExclusionRule> getDetectionExclusionRules(
      RequestContext requestContext, GetRulesFilter filter) {
    rulesMigrationManager.migrateFromOldStoreIfApplicable(requestContext);
    rulesMigrationManager.migrateFromChangeLog2IfApplicable(requestContext);
    rulesMigrationManager.migrateFromChangeLog3IfApplicable(requestContext);
    if (filter.equals(GetRulesFilter.getDefaultInstance())) {
      return rulesStore.getAllConfigData(requestContext);
    }
    return rulesStore.getAllConfigData(requestContext, filter);
  }

  @Override
  public DetectionExclusionRule updateDetectionExclusionRule(
      RequestContext requestContext, DetectionExclusionRule rule) {
    String ruleId = rule.getId();
    List<DetectionExclusionRule> ruleList = rulesStore.getAllConfigData(requestContext);
    DetectionExclusionRule originalRule =
        ruleList.stream()
            .filter(detectionExclusionRule -> detectionExclusionRule.getId().equals(ruleId))
            .findFirst()
            .orElseThrow(
                Status.NOT_FOUND.withDescription(
                        String.format(
                            "Detection exclusion rule with rule id : {} does not exists", ruleId))
                    ::asRuntimeException);
    rule = processDetectionExclusionRule(rule, originalRule.getRuleInfo().getRuleStatus());
    return rulesStore.upsertObject(requestContext, rule).getData();
  }

  @Override
  public DetectionExclusionRule createDetectionExclusionRule(
      RequestContext requestContext,
      DetectionExclusionRuleScope ruleScope,
      DetectionExclusionRuleInfo ruleInfo) {
    DetectionExclusionRule rule =
        DetectionExclusionRule.newBuilder()
            .setId(uuidGenerator.generateRandomId())
            .setRuleInfo(processDetectionExclusionRuleInfo(ruleInfo))
            .setRuleScope(ruleScope)
            .build();
    return rulesStore.upsertObject(requestContext, rule).getData();
  }

  @Override
  public void deleteDetectionExclusionRule(RequestContext requestContext, String ruleId) {
    rulesStore
        .deleteObject(requestContext, ruleId)
        .orElseThrow(() -> Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers()));
  }

  private DetectionExclusionRule processDetectionExclusionRule(
      DetectionExclusionRule rule, DetectionExclusionRuleStatus originalRuleStatus) {
    DetectionExclusionRuleInfo processedDetectionExclusionRuleInfo =
        processDetectionExclusionRuleInfo(rule.getRuleInfo());
    rule = rule.toBuilder().setRuleInfo(processedDetectionExclusionRuleInfo).build();
    return getModifiedRule(rule, originalRuleStatus);
  }

  private DetectionExclusionRuleInfo processDetectionExclusionRuleInfo(
      DetectionExclusionRuleInfo ruleInfo) {
    DetectionExclusionRuleInfo.Builder builder = ruleInfo.toBuilder();
    processIpAddressCondition(ruleInfo, builder);
    processRuleExpiration(ruleInfo, builder);
    processExclusionTarget(ruleInfo, builder);
    return builder.build();
  }

  // added EXCLUSION_TARGET_ALERT to exclusion target list (if list is empty), for backward
  // compatibility.
  private void processExclusionTarget(
      DetectionExclusionRuleInfo ruleInfo, DetectionExclusionRuleInfo.Builder builder) {
    if (ruleInfo.getExclusionTargetsList().isEmpty()) {
      builder.addExclusionTargets(EXCLUSION_TARGET_ALERT);
    }
  }

  private void processIpAddressCondition(
      DetectionExclusionRuleInfo ruleInfo, DetectionExclusionRuleInfo.Builder builder) {
    List<DetectionExclusionCondition> detectionExclusionConditions =
        ruleInfo.getConditionsList().stream()
            .map(
                detectionExclusionCondition -> {
                  DetectionExclusionCondition.Builder conditionBuilder =
                      detectionExclusionCondition.toBuilder();
                  if (detectionExclusionCondition.hasIpAddressCondition()) {
                    IpAddressCondition ipAddressCondition =
                        detectionExclusionCondition.getIpAddressCondition();
                    List<String> rawIps = ipAddressCondition.getRawInputIpDataList();
                    IpAddressParsingUtils.IpParsingResults parsedResults = parseRawIpRange(rawIps);
                    IpAddressCondition.Builder ipAddressConditionBuilder =
                        detectionExclusionCondition.toBuilder().getIpAddressConditionBuilder();
                    ipAddressConditionBuilder
                        .addAllCidrIpRanges(parsedResults.getIpRanges())
                        .addAllIpAddresses(parsedResults.getIpAddresses());
                    conditionBuilder.setIpAddressCondition(ipAddressConditionBuilder);
                  }
                  return conditionBuilder.build();
                })
            .collect(Collectors.toUnmodifiableList());
    builder.clearConditions();
    builder.addAllConditions(detectionExclusionConditions);
  }

  private void processRuleExpiration(
      DetectionExclusionRuleInfo ruleInfo, DetectionExclusionRuleInfo.Builder builder) {
    DetectionExclusionRuleStatus ruleStatus = ruleInfo.getRuleStatus();
    if (ruleStatus.hasExpirationDetails()
        && !ruleStatus.getExpirationDetails().hasExpirationTimestampMillis()) {
      builder
          .getRuleStatusBuilder()
          .getExpirationDetailsBuilder()
          .setExpirationTimestampMillis(
              clock.millis()
                  + Duration.parse(ruleStatus.getExpirationDetails().getExpirationDuration())
                      .toMillis());
    }
  }

  private DetectionExclusionRuleStatus getMergeRuleStatus(
      DetectionExclusionRuleStatus ruleStatus, DetectionExclusionRuleStatus originalRuleStatus) {
    return originalRuleStatus.toBuilder()
        .mergeFrom(ruleStatus)
        .setDisabled(ruleStatus.getDisabled())
        .setRuleCreationSource(originalRuleStatus.getRuleCreationSource())
        .build();
  }

  private DetectionExclusionRule getModifiedRule(
      DetectionExclusionRule rule, DetectionExclusionRuleStatus originalRuleStatus) {
    DetectionExclusionRuleStatus mergedRuleStatus =
        getMergeRuleStatus(rule.getRuleInfo().getRuleStatus(), originalRuleStatus);
    DetectionExclusionRuleInfo modifiedRuleInfo =
        rule.getRuleInfo().toBuilder().setRuleStatus(mergedRuleStatus).build();
    rule = rule.toBuilder().setRuleInfo(modifiedRuleInfo).build();
    return rule;
  }
}
