package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;

@UtilityClass
public class DetectionExclusionRuleIdMigrationManager {
  private static final Map<String, String> OLD_TO_NEW_RULE_ID_MAPPINGS = new HashMap<>();

  static {
    OLD_TO_NEW_RULE_ID_MAPPINGS.put("crs_9210310", "crs_9320310");
    // Add future migrations here
  }

  public static boolean updateRuleConditions(DetectionExclusionRuleInfo.Builder ruleInfoBuilder) {
    boolean isUpdated = false;
    List<DetectionExclusionCondition> conditions = new ArrayList<>();

    for (DetectionExclusionCondition condition : ruleInfoBuilder.getConditionsList()) {
      if (hasSystemDefinedEventBasedConditionNeedingMigration(condition)) {
        List<SystemDefinedEvent> migratedEvents = migrateSystemDefinedEvents(condition);
        conditions.add(
            condition.toBuilder()
                .setEventCondition(
                    condition.getEventCondition().toBuilder()
                        .clearSystemDefinedEvents()
                        .addAllSystemDefinedEvents(migratedEvents))
                .build());
        isUpdated = true;
      } else {
        conditions.add(condition);
      }
    }

    if (isUpdated) {
      ruleInfoBuilder.clearConditions();
      ruleInfoBuilder.addAllConditions(conditions);
    }

    return isUpdated;
  }

  private static boolean hasSystemDefinedEventBasedConditionNeedingMigration(
      DetectionExclusionCondition condition) {
    return condition.getEventCondition().getSystemDefinedEventsList().stream()
        .anyMatch(event -> needsMigration(event.getEventSubTypeId()));
  }

  private static List<SystemDefinedEvent> migrateSystemDefinedEvents(
      DetectionExclusionCondition condition) {
    return condition.getEventCondition().getSystemDefinedEventsList().stream()
        .map(
            event -> {
              String oldRuleId = event.getEventSubTypeId();
              if (needsMigration(oldRuleId)) {
                return event.toBuilder().setEventSubTypeId(getMigratedRuleId(oldRuleId)).build();
              }
              return event;
            })
        .collect(Collectors.toList());
  }

  private static String getMigratedRuleId(String oldRuleId) {
    return OLD_TO_NEW_RULE_ID_MAPPINGS.getOrDefault(oldRuleId, oldRuleId);
  }

  private static boolean needsMigration(String ruleId) {
    return OLD_TO_NEW_RULE_ID_MAPPINGS.containsKey(ruleId);
  }
}
