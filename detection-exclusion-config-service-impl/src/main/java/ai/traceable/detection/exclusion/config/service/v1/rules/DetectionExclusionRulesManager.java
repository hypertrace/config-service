package ai.traceable.detection.exclusion.config.service.v1.rules;

import static ai.traceable.detection.exclusion.config.service.v1.ExclusionTarget.EXCLUSION_TARGET_ALERT;
import static ai.traceable.platform.utils.ip.IpAddressParsingUtils.parseRawIpRange;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleRecord;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesRequest;
import ai.traceable.detection.exclusion.config.service.v1.GetExclusionModsecRulesResponse;
import ai.traceable.detection.exclusion.config.service.v1.GetRulesFilter;
import ai.traceable.detection.exclusion.config.service.v1.IpAddressCondition;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import ai.traceable.detection.exclusion.config.service.v1.UpsertDetectionExclusionRuleData;
import ai.traceable.detection.exclusion.config.service.v1.rules.migration.RulesMigrationManager;
import ai.traceable.detection.exclusion.config.service.v1.rules.modsec.ExclusionModsecRulesManager;
import ai.traceable.platform.utils.ip.IpAddressParsingUtils;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ConfigObject;
import org.hypertrace.config.objectstore.IdentifiedObjectStoreWithFilter;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class DetectionExclusionRulesManager implements RulesManager {
  private final DetectionExclusionRulesStore rulesStore;
  private final ThresholdExceededDetectionExclusionRuleStore
      thresholdExceededDetectionExclusionRuleStore;
  private final UuidGenerator uuidGenerator;
  private final RulesMigrationManager rulesMigrationManager;
  private final ExclusionModsecRulesManager exclusionModsecRulesManager;
  private final Clock clock;

  @Inject
  public DetectionExclusionRulesManager(
      DetectionExclusionRulesStore rulesStore,
      ThresholdExceededDetectionExclusionRuleStore thresholdExceededDetectionExclusionRuleStore,
      UuidGenerator uuidGenerator,
      RulesMigrationManager rulesMigrationManager,
      ExclusionModsecRulesManager exclusionModsecRulesManager,
      Clock clock) {
    this.rulesStore = rulesStore;
    this.thresholdExceededDetectionExclusionRuleStore =
        thresholdExceededDetectionExclusionRuleStore;
    this.uuidGenerator = uuidGenerator;
    this.rulesMigrationManager = rulesMigrationManager;
    this.exclusionModsecRulesManager = exclusionModsecRulesManager;
    this.clock = clock;
  }

  @Override
  public List<DetectionExclusionRule> getDetectionExclusionRules(
      RequestContext requestContext, GetRulesFilter filter) {
    return getDetectionExclusionRuleRecords(requestContext, filter).stream()
        .map(DetectionExclusionRuleRecord::getRule)
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<DetectionExclusionRuleRecord> getDetectionExclusionRuleRecords(
      RequestContext requestContext, GetRulesFilter filter) {
    runMigrations(requestContext);

    List<DetectionExclusionRuleRecord> records = new ArrayList<>();
    if (filter.equals(GetRulesFilter.getDefaultInstance())) {
      records.addAll(rulesStore.getAllRuleRecords(requestContext));
      return records;
    }
    records.addAll(rulesStore.getAllRuleRecords(requestContext, filter));
    if (filter
        .getRuleCreationSourcesList()
        .contains(RuleSource.RULE_SOURCE_COUNT_THRESHOLD_EXCEEDED)) {
      records.addAll(
          thresholdExceededDetectionExclusionRuleStore.getAllRuleRecords(requestContext, filter));
    }
    return records;
  }

  private void runMigrations(RequestContext requestContext) {
    rulesMigrationManager.migrateFromOldStoreIfApplicable(requestContext);
    rulesMigrationManager.migrateFromChangeLog2IfApplicable(requestContext);
    rulesMigrationManager.migrateFromChangeLog3IfApplicable(requestContext);
    rulesMigrationManager.migrateFromChangeLog4IfApplicable(requestContext);
    rulesMigrationManager.migrateForRuleEvaluationPointsIfApplicable(requestContext);
    rulesMigrationManager.migrateForApiProtectionExclusionRulesIfApplicable(requestContext);
    rulesMigrationManager.migrateForAllowOnlyPlatformRemovalIfApplicable(requestContext);
  }

  @Override
  public DetectionExclusionRule updateDetectionExclusionRule(
      RequestContext requestContext, DetectionExclusionRule rule) {
    String ruleId = rule.getId();
    List<DetectionExclusionRule> ruleList;
    DetectionExclusionRule originalRule;
    IdentifiedObjectStoreWithFilter<DetectionExclusionRule, GetRulesFilter> ruleStore =
        getRuleStore(rule.getRuleInfo().getRuleStatus().getRuleCreationSource());
    ruleList = ruleStore.getAllConfigData(requestContext);
    originalRule =
        ruleList.stream()
            .filter(detectionExclusionRule -> detectionExclusionRule.getId().equals(ruleId))
            .findFirst()
            .orElseThrow(
                () ->
                    Status.NOT_FOUND
                        .withDescription(
                            String.format(
                                "Detection exclusion rule with rule id : %s does not exists",
                                ruleId))
                        .asRuntimeException());
    rule = processDetectionExclusionRule(rule, originalRule.getRuleInfo().getRuleStatus());
    return ruleStore.upsertObject(requestContext, rule).getData();
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
  public List<DetectionExclusionRule> bulkUpsertDetectionExclusionRule(
      RequestContext requestContext, List<UpsertDetectionExclusionRuleData> ruleDataList) {
    List<UpsertDetectionExclusionRuleData> thresholdExceededRuleDataList =
        ruleDataList.stream()
            .filter(
                rule ->
                    rule.getRuleInfo()
                        .getRuleStatus()
                        .getRuleCreationSource()
                        .equals(RuleSource.RULE_SOURCE_COUNT_THRESHOLD_EXCEEDED))
            .collect(Collectors.toUnmodifiableList());
    List<UpsertDetectionExclusionRuleData> otherRuleDataList =
        ruleDataList.stream()
            .filter(
                rule ->
                    !rule.getRuleInfo()
                        .getRuleStatus()
                        .getRuleCreationSource()
                        .equals(RuleSource.RULE_SOURCE_COUNT_THRESHOLD_EXCEEDED))
            .collect(Collectors.toUnmodifiableList());

    List<DetectionExclusionRule> rules = new ArrayList<>();
    List<DetectionExclusionRule> thresholdExceededRules =
        getRulesWithIdsPopulated(thresholdExceededRuleDataList);
    if (!thresholdExceededRules.isEmpty()) {
      rules.addAll(
          thresholdExceededDetectionExclusionRuleStore
              .upsertObjects(requestContext, thresholdExceededRules)
              .stream()
              .map(ConfigObject::getData)
              .collect(Collectors.toUnmodifiableList()));
    }
    List<DetectionExclusionRule> otherRules = getRulesWithIdsPopulated(otherRuleDataList);
    if (!otherRules.isEmpty()) {
      rules.addAll(
          rulesStore.upsertObjects(requestContext, otherRules).stream()
              .map(ConfigObject::getData)
              .collect(Collectors.toUnmodifiableList()));
    }
    return rules;
  }

  @Override
  public void deleteDetectionExclusionRule(RequestContext requestContext, String ruleId) {
    if (rulesStore.deleteObject(requestContext, ruleId).isEmpty()
        && thresholdExceededDetectionExclusionRuleStore
            .deleteObject(requestContext, ruleId)
            .isEmpty()
        && !isDefaultRule(requestContext, ruleId)) {
      throw Status.NOT_FOUND.asRuntimeException(requestContext.buildTrailers());
    }
  }

  private List<DetectionExclusionRule> getRulesWithIdsPopulated(
      List<UpsertDetectionExclusionRuleData> rules) {
    return rules.stream()
        .map(
            request -> {
              String ruleId;
              switch (request.getIdCase()) {
                case PRE_DEFINED_ID:
                  ruleId = request.getPreDefinedId();
                  break;
                case UUID_FROM_NAME_AND_SOURCE:
                  ruleId =
                      uuidGenerator.generateId(
                          request.getRuleInfo().getName()
                              + request
                                  .getRuleInfo()
                                  .getRuleStatus()
                                  .getRuleCreationSource()
                                  .name());
                  break;
                case ID_NOT_SET:
                default:
                  ruleId = uuidGenerator.generateRandomId();
              }
              return DetectionExclusionRule.newBuilder()
                  .setId(ruleId)
                  .setRuleInfo(processDetectionExclusionRuleInfo(request.getRuleInfo()))
                  .setRuleScope(request.getRuleScope())
                  .build();
            })
        .collect(Collectors.toUnmodifiableList());
  }

  private boolean isDefaultRule(RequestContext requestContext, String ruleId) {
    return rulesStore
        .getData(requestContext, ruleId)
        .map(
            rule ->
                rule.getRuleInfo()
                    .getRuleStatus()
                    .getRuleCreationSource()
                    .equals(RuleSource.RULE_SOURCE_DEFAULT))
        .orElse(false);
  }

  @Override
  public GetExclusionModsecRulesResponse getDetectionExclusionModsecRules(
      RequestContext requestContext, GetExclusionModsecRulesRequest request) {
    return exclusionModsecRulesManager.getModsecRules(
        requestContext,
        getDetectionExclusionRules(requestContext, request.getRulesFilter()),
        request.getServiceNamesList());
  }

  @Override
  public void bulkDeleteDetectionExclusionRules(
      RequestContext requestContext, List<String> ruleIds) {
    rulesStore.deleteObjects(requestContext, ruleIds);
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

  // added EXCLUSION_TARGET_ALERT to exclusionTargetList (if exclusionTargetList is empty), for
  // backward
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

  private IdentifiedObjectStoreWithFilter<DetectionExclusionRule, GetRulesFilter> getRuleStore(
      RuleSource ruleSource) {
    if (ruleSource.equals(RuleSource.RULE_SOURCE_COUNT_THRESHOLD_EXCEEDED)) {
      return thresholdExceededDetectionExclusionRuleStore;
    }
    return rulesStore;
  }
}
