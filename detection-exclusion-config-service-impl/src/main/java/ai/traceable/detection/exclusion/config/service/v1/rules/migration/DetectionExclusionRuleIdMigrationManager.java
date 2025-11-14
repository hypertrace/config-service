package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import ai.traceable.anomaly.config.service.v1.ApiProtectThreatRuleConfigMappingProvider;
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
  private static final Map<String, List<String>> OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS = new HashMap<>();

  static {
    OLD_TO_NEW_RULE_ID_MAPPINGS.put("crs_9210310", "crs_9320310");

    OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.put(
        ApiProtectThreatRuleConfigMappingProvider.BOLA_THREAT_TYPE_ID,
        List.of(ApiProtectThreatRuleConfigMappingProvider.AUTHZH_BOLA_SUB_RULE_ID));
    OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.put(
        ApiProtectThreatRuleConfigMappingProvider.USER_ID_BOLA_THREAT_TYPE_ID,
        List.of(ApiProtectThreatRuleConfigMappingProvider.AUTHZH_USER_ID_BOLA_SUB_RULE_ID));
    OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.put(
        ApiProtectThreatRuleConfigMappingProvider.BFLA_THREAT_TYPE_ID,
        List.of(ApiProtectThreatRuleConfigMappingProvider.AUTHZV_BFLA_SUB_RULE_ID));

    OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.put(
        ApiProtectThreatRuleConfigMappingProvider.CONTENT_SIZE_THREAT_TYPE_ID,
        List.of(
            ApiProtectThreatRuleConfigMappingProvider.CONTENT_ANOMALY_UREQCL_SUB_RULE_ID,
            ApiProtectThreatRuleConfigMappingProvider.CONTENT_ANOMALY_URESCL_SUB_RULE_ID));
    OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.put(
        ApiProtectThreatRuleConfigMappingProvider.CONTENT_TYPE_THREAT_TYPE_ID,
        List.of(
            ApiProtectThreatRuleConfigMappingProvider.CONTENT_ANOMALY_REQCTM_SUB_RULE_ID,
            ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_REQCTVE_SUB_RULE_ID,
            ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_RESCTVE_SUB_RULE_ID));
    OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.put(
        ApiProtectThreatRuleConfigMappingProvider.CONTENT_EXPLOSION_THREAT_TYPE_ID,
        List.of(ApiProtectThreatRuleConfigMappingProvider.CONTENT_ANOMALY_REQCE_SUB_RULE_ID));

    OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.put(
        ApiProtectThreatRuleConfigMappingProvider.SPECIAL_CHARACTER_THREAT_TYPE_ID,
        List.of(ApiProtectThreatRuleConfigMappingProvider.PARAMETER_ANOMALY_SCT_SUB_RULE_ID));
    OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.put(
        ApiProtectThreatRuleConfigMappingProvider.INTEGER_THREAT_TYPE_ID,
        List.of(
            ApiProtectThreatRuleConfigMappingProvider.PARAMETER_ANOMALY_INTVOR_SUB_RULE_ID,
            ApiProtectThreatRuleConfigMappingProvider.PARAMETER_ANOMALY_INTUNS_SUB_RULE_ID));
    OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.put(
        ApiProtectThreatRuleConfigMappingProvider.DEVICE_THREAT_TYPE_ID,
        List.of(ApiProtectThreatRuleConfigMappingProvider.PARAMETER_ANOMALY_UUAD_SUB_RULE_ID));

    OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.put(
        ApiProtectThreatRuleConfigMappingProvider.ENUM_THREAT_TYPE_ID,
        List.of(ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_REQIE_SUB_RULE_ID));
    OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.put(
        ApiProtectThreatRuleConfigMappingProvider.UNKNOWN_PARAM_THREAT_TYPE_ID,
        List.of(ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_UREQP_SUB_RULE_ID));
    OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.put(
        ApiProtectThreatRuleConfigMappingProvider.TYPE_THREAT_TYPE_ID,
        List.of(ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_REQPTVE_SUB_RULE_ID));
    OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.put(
        ApiProtectThreatRuleConfigMappingProvider.HTTP_STATUS_THREAT_TYPE_ID,
        List.of(ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_URESC_SUB_RULE_ID));
    OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.put(
        ApiProtectThreatRuleConfigMappingProvider.MISSING_PARAM_THREAT_TYPE_ID,
        List.of(
            ApiProtectThreatRuleConfigMappingProvider.CSTA_CSRF_SUB_RULE_ID,
            ApiProtectThreatRuleConfigMappingProvider.AUTHN_AUA_SUB_RULE_ID,
            ApiProtectThreatRuleConfigMappingProvider.AUTHN_UA_SUB_RULE_ID,
            ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_MREQP_SUB_RULE_ID));

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

  public static boolean updateRuleConditionsExclusionRulesForApiProtection(
      DetectionExclusionRuleInfo.Builder ruleInfoBuilder) {
    boolean isUpdated = false;
    List<DetectionExclusionCondition> conditions = new ArrayList<>();
    for (DetectionExclusionCondition condition : ruleInfoBuilder.getConditionsList()) {
      if (hasSystemDefinedEventBasedConditionNeedingMigrationForApiProtection(condition)) {
        List<SystemDefinedEvent> migratedEvents =
            migrateSystemDefinedEventsForApiProtection(condition);
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

  private static boolean hasSystemDefinedEventBasedConditionNeedingMigrationForApiProtection(
      DetectionExclusionCondition condition) {
    return condition.getEventCondition().getSystemDefinedEventsList().stream()
        .anyMatch(event -> needsTypeIdMigration(event.getEventTypeId()));
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

  private static List<SystemDefinedEvent> migrateSystemDefinedEventsForApiProtection(
      DetectionExclusionCondition condition) {
    List<SystemDefinedEvent> migratedEvents = new ArrayList<>();

    for (SystemDefinedEvent event : condition.getEventCondition().getSystemDefinedEventsList()) {
      if (needsTypeIdMigration(event.getEventTypeId())) {
        List<String> newSubTypeIds = OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.get(event.getEventTypeId());
        for (String newSubTypeId : newSubTypeIds) {
          migratedEvents.add(
              event.toBuilder().clearEventTypeId().setEventSubTypeId(newSubTypeId).build());
        }
      } else {
        migratedEvents.add(event);
      }
    }
    return migratedEvents;
  }

  private static String getMigratedRuleId(String oldRuleId) {
    return OLD_TO_NEW_RULE_ID_MAPPINGS.getOrDefault(oldRuleId, oldRuleId);
  }

  private static boolean needsMigration(String ruleId) {
    return OLD_TO_NEW_RULE_ID_MAPPINGS.containsKey(ruleId);
  }

  private static boolean needsTypeIdMigration(String typeId) {
    return OLD_TYPE_ID_TO_NEW_SUB_TYPE_IDS.containsKey(typeId);
  }
}
