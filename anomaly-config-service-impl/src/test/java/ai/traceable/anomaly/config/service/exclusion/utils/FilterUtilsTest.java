package ai.traceable.anomaly.config.service.exclusion.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActor;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActorExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionType;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class FilterUtilsTest {
  private FilterUtils filterUtils = new FilterUtils();

  @Test
  void testFilterRuleIds() {
    AnomalyExclusionRuleConfig mockConfig =
        AnomalyExclusionRuleConfig.newBuilder().setId("rule_id").build();
    assertTrue(filterUtils.filterRuleIds(mockConfig, Set.of("rule_id")));
    assertFalse(filterUtils.filterRuleIds(mockConfig, Set.of("other_id")));
    assertTrue(filterUtils.filterRuleIds(mockConfig, Set.of()));
  }

  @Test
  void testFilterEventIds() {
    EventExclusionInfo eventExclusionInfoAllEvents =
        EventExclusionInfo.newBuilder()
            .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_ALL_EVENTS)
            .build();

    AnomalyExclusionRuleConfig mockConfigAllEvents =
        AnomalyExclusionRuleConfig.newBuilder()
            .setRuleData(
                AnomalyExclusionRuleData.newBuilder()
                    .setEventExclusionInfo(eventExclusionInfoAllEvents))
            .build();
    assertTrue(filterUtils.filterEventIds(mockConfigAllEvents, List.of("bola")));

    AnomalyExclusionRuleConfig mockConfigModSecRuleType =
        AnomalyExclusionRuleConfig.newBuilder()
            .setRuleData(
                AnomalyExclusionRuleData.newBuilder()
                    .setEventExclusionInfo(
                        EventExclusionInfo.newBuilder()
                            .setEventExclusionType(
                                EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE)
                            .setEventTypeId("crs_111")
                            .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
                            .build()))
            .build();
    assertTrue(filterUtils.filterEventIds(mockConfigModSecRuleType, List.of("crs_111000")));

    AnomalyExclusionRuleConfig mockConfigModSec =
        AnomalyExclusionRuleConfig.newBuilder()
            .setRuleData(
                AnomalyExclusionRuleData.newBuilder()
                    .setEventExclusionInfo(
                        EventExclusionInfo.newBuilder()
                            .setEventExclusionType(
                                EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_SUBTYPE)
                            .setEventTypeId("crs_111000")
                            .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
                            .build()))
            .build();
    assertTrue(filterUtils.filterEventIds(mockConfigModSec, List.of("crs_111000")));
    assertFalse(filterUtils.filterEventIds(mockConfigModSec, List.of("crs_111111")));
    assertTrue(filterUtils.filterEventIds(mockConfigModSec, List.of()));
  }

  @Test
  void testFilterEventFamilies() {
    EventExclusionInfo eventExclusionInfoAllEvents =
        EventExclusionInfo.newBuilder()
            .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_ALL_EVENTS)
            .build();

    AnomalyExclusionRuleConfig mockConfigAllEvents =
        AnomalyExclusionRuleConfig.newBuilder()
            .setRuleData(
                AnomalyExclusionRuleData.newBuilder()
                    .setEventExclusionInfo(eventExclusionInfoAllEvents))
            .build();

    AnomalyExclusionRuleConfig mockConfigModSecRuleType =
        AnomalyExclusionRuleConfig.newBuilder()
            .setRuleData(
                AnomalyExclusionRuleData.newBuilder()
                    .setEventExclusionInfo(
                        EventExclusionInfo.newBuilder()
                            .setEventExclusionType(
                                EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE)
                            .setEventTypeId("crs_111")
                            .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
                            .build()))
            .build();

    assertTrue(
        filterUtils.filterEventFamilies(
            mockConfigModSecRuleType, List.of(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)));
    assertFalse(
        filterUtils.filterEventFamilies(
            mockConfigModSecRuleType, List.of(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION)));
    assertTrue(filterUtils.filterEventFamilies(mockConfigModSecRuleType, List.of()));
    assertTrue(
        filterUtils.filterEventFamilies(
            mockConfigAllEvents, List.of(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION)));
  }

  @Test
  void testFilterAnomalyActorIds() {
    AnomalyActorExclusionInfo all_actor_info = AnomalyActorExclusionInfo.newBuilder().build();

    AnomalyExclusionRuleConfig mockConfigAllActors =
        AnomalyExclusionRuleConfig.newBuilder()
            .setRuleData(
                AnomalyExclusionRuleData.newBuilder().setAnomalyActorExclusionInfo(all_actor_info))
            .build();

    AnomalyExclusionRuleConfig mockConfigSpecificActor =
        AnomalyExclusionRuleConfig.newBuilder()
            .setRuleData(
                AnomalyExclusionRuleData.newBuilder()
                    .setAnomalyActorExclusionInfo(
                        AnomalyActorExclusionInfo.newBuilder()
                            .setAnomalyActor(AnomalyActor.newBuilder().setId("actor_id"))))
            .build();
    assertTrue(filterUtils.filterAnomalyActorIds(mockConfigAllActors, Set.of("actor_id")));
    assertTrue(filterUtils.filterAnomalyActorIds(mockConfigSpecificActor, Set.of("actor_id")));
    assertFalse(filterUtils.filterAnomalyActorIds(mockConfigSpecificActor, Set.of("some_id")));
    assertTrue(filterUtils.filterAnomalyActorIds(mockConfigSpecificActor, Set.of()));
  }
}
