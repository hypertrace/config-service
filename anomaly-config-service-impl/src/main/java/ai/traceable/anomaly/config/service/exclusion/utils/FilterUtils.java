package ai.traceable.anomaly.config.service.exclusion.utils;

import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActorExclusionInfo.AnomalyActorExclusionCase;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionType;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class FilterUtils {

  public boolean filterRuleIds(AnomalyExclusionRuleConfig config, Set<String> ruleIds) {
    if (isUnsetFilter(ruleIds)) {
      return true;
    }
    return ruleIds.contains(config.getId());
  }

  public boolean filterEventIds(AnomalyExclusionRuleConfig config, List<String> eventTypeIds) {
    EventExclusionInfo eventExclusionInfo = config.getRuleData().getEventExclusionInfo();
    if (isUnsetFilter(eventTypeIds)
        || EventExclusionType.EVENT_EXCLUSION_TYPE_ALL_EVENTS.equals(
            eventExclusionInfo.getEventExclusionType())) {
      return true;
    }
    if (AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC.equals(
            eventExclusionInfo.getAnomalyEventFamily())
        && EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE.equals(
            eventExclusionInfo.getEventExclusionType())) {
      return eventTypeIds.stream()
          .anyMatch(eventTypeId -> eventTypeId.startsWith(eventExclusionInfo.getEventTypeId()));
    }

    return eventTypeIds.contains(config.getRuleData().getEventExclusionInfo().getEventTypeId());
  }

  public boolean filterEventFamilies(
      AnomalyExclusionRuleConfig config, List<AnomalyEventFamily> anomalyEventFamilies) {
    if (isUnsetFilter(anomalyEventFamilies)
        || EventExclusionType.EVENT_EXCLUSION_TYPE_ALL_EVENTS.equals(
            config.getRuleData().getEventExclusionInfo().getEventExclusionType())) {
      return true;
    }
    return anomalyEventFamilies.contains(
        config.getRuleData().getEventExclusionInfo().getAnomalyEventFamily());
  }

  public boolean filterAnomalyActorIds(
      AnomalyExclusionRuleConfig config, Set<String> threatActorIds) {
    if (isUnsetFilter(threatActorIds)
        || AnomalyActorExclusionCase.ANOMALYACTOREXCLUSION_NOT_SET.equals(
            config.getRuleData().getAnomalyActorExclusionInfo().getAnomalyActorExclusionCase())) {
      return true;
    }
    return threatActorIds.contains(
        config.getRuleData().getAnomalyActorExclusionInfo().getAnomalyActor().getId());
  }

  private boolean isUnsetFilter(Collection<?> filterValues) {
    return filterValues.isEmpty();
  }
}
