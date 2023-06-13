package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import static ai.traceable.anomaly.config.service.v1.AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF;
import static ai.traceable.anomaly.config.service.v1.AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC;
import static ai.traceable.anomaly.config.service.v1.AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION;
import static ai.traceable.detection.exclusion.config.service.v1.CustomRuleFamily.CUSTOM_RULE_FAMILY_SIGNATURE;
import static ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_API_DEF;
import static ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_MODSEC;
import static ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily.SYSTEM_DEFINED_EVENT_FAMILY_SESSION;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyParamInfo;
import ai.traceable.anomaly.config.service.v1.AnomalyParamInfoScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionType;
import ai.traceable.detection.exclusion.config.service.v1.AnomalousAttributeCondition;
import ai.traceable.detection.exclusion.config.service.v1.CustomRuleEvent;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionCondition;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRule;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleInfo;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleScope;
import ai.traceable.detection.exclusion.config.service.v1.DetectionExclusionRuleStatus;
import ai.traceable.detection.exclusion.config.service.v1.EntityScope;
import ai.traceable.detection.exclusion.config.service.v1.EntityType;
import ai.traceable.detection.exclusion.config.service.v1.EnvironmentScope;
import ai.traceable.detection.exclusion.config.service.v1.EventCondition;
import ai.traceable.detection.exclusion.config.service.v1.MatchCondition;
import ai.traceable.detection.exclusion.config.service.v1.MatchOperator;
import ai.traceable.detection.exclusion.config.service.v1.RuleSource;
import ai.traceable.detection.exclusion.config.service.v1.ScopeCondition;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEvent;
import ai.traceable.detection.exclusion.config.service.v1.SystemDefinedEventFamily;
import ai.traceable.detection.exclusion.config.service.v1.UserIdCondition;
import com.google.protobuf.Value;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DetectionExclusionRuleConverter {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DetectionExclusionRuleConverter.class);

  private static final DetectionExclusionRuleStatus DEFAULT_OLD_API_RULE_STATUS =
      DetectionExclusionRuleStatus.newBuilder()
          .setRuleCreationSource(RuleSource.RULE_SOURCE_OLD_API)
          .build();

  private static final Map<AnomalyEventFamily, SystemDefinedEventFamily>
      systemDefinedEventFamilyMap =
          Map.of(
              ANOMALY_EVENT_FAMILY_API_DEF,
              SYSTEM_DEFINED_EVENT_FAMILY_API_DEF,
              ANOMALY_EVENT_FAMILY_SESSION,
              SYSTEM_DEFINED_EVENT_FAMILY_SESSION,
              ANOMALY_EVENT_FAMILY_MODSEC,
              SYSTEM_DEFINED_EVENT_FAMILY_MODSEC);

  private static final EventCondition ALL_SYSTEM_DEFINED_EVENTS_CONDITION =
      EventCondition.newBuilder()
          .addAllSystemDefinedEvents(
              List.of(
                      SYSTEM_DEFINED_EVENT_FAMILY_API_DEF,
                      SYSTEM_DEFINED_EVENT_FAMILY_MODSEC,
                      SYSTEM_DEFINED_EVENT_FAMILY_SESSION)
                  .stream()
                  .map(family -> SystemDefinedEvent.newBuilder().setEventFamily(family).build())
                  .collect(Collectors.toList()))
          .build();

  DetectionExclusionRule convertRule(
      AnomalyExclusionRuleConfig oldRule, Map<String, String> oldRulesActorEntityIdToIdMap) {
    try {
      DetectionExclusionRule.Builder ruleBuilder =
          DetectionExclusionRule.newBuilder().setId(oldRule.getId());

      AnomalyConfigStatus oldRuleStatus = oldRule.getConfigStatus();
      AnomalyExclusionRuleData oldRuleData = oldRule.getRuleData();

      DetectionExclusionRuleStatus.Builder ruleStatusBuilder =
          DEFAULT_OLD_API_RULE_STATUS.toBuilder()
              .setDisabled(oldRuleStatus.getDisabled())
              .setGenerateInternalEvents(oldRuleStatus.getInternal());

      DetectionExclusionRuleInfo.Builder ruleInfoBuilder =
          DetectionExclusionRuleInfo.newBuilder()
              .setName(oldRuleData.getName())
              .setDescription(oldRuleData.getDescription());

      ruleInfoBuilder.addConditions(
          DetectionExclusionCondition.newBuilder()
              .setEventCondition(getEventCondition(oldRuleData.getEventExclusionInfo())));

      AnomalyConfigScope anomalyConfigScope = oldRuleData.getAnomalyConfigScope();
      getRuleScope(anomalyConfigScope).ifPresent(ruleBuilder::setRuleScope);
      getScopeCondition(anomalyConfigScope)
          .ifPresent(
              scopeCondition ->
                  ruleInfoBuilder.addConditions(
                      DetectionExclusionCondition.newBuilder().setScopeCondition(scopeCondition)));
      if (anomalyConfigScope.hasParamScope()) {
        ruleInfoBuilder.addConditions(
            DetectionExclusionCondition.newBuilder()
                .setAnomalousAttributeCondition(
                    getAnomalousAttributeCondition(anomalyConfigScope.getParamScope())));
      }

      AnomalyConfigScope sourceConfigScope = oldRuleData.getSourceConfigScope();
      getScopeCondition(sourceConfigScope)
          .ifPresent(
              scopeCondition -> {
                ruleInfoBuilder.addConditions(
                    DetectionExclusionCondition.newBuilder()
                        .setSourceScopeCondition(scopeCondition));
                ruleStatusBuilder.setHidden(true);
              });
      if (sourceConfigScope.hasParamScope()) {
        ruleInfoBuilder.addConditions(
            DetectionExclusionCondition.newBuilder()
                .setSourceAnomalousAttributeMatchCondition(
                    getAnomalousAttributeCondition(sourceConfigScope.getParamScope())));
        ruleStatusBuilder.setHidden(true);
      }

      if (oldRuleData.getAnomalyActorExclusionInfo().hasAnomalyActor()) {
        String actorEntityId = oldRuleData.getAnomalyActorExclusionInfo().getAnomalyActor().getId();
        Optional<String> actorId =
            Optional.ofNullable(oldRulesActorEntityIdToIdMap.get(actorEntityId));
        if (actorId.isPresent()) {
          ruleInfoBuilder.addConditions(
              DetectionExclusionCondition.newBuilder()
                  .setUserIdCondition(UserIdCondition.newBuilder().addUserIds(actorId.get())));
        } else {
          throw Status.NOT_FOUND
              .withDescription(
                  String.format("ActorId not found for ActorEntityId : %s", actorEntityId))
              .asRuntimeException();
        }
      }

      return ruleBuilder.setRuleInfo(ruleInfoBuilder.setRuleStatus(ruleStatusBuilder)).build();
    } catch (Exception e) {
      LOGGER.warn("Exception in converting old rule ID: {}", oldRule.getId(), e);
      return null;
    }
  }

  private Optional<DetectionExclusionRuleScope> getRuleScope(
      AnomalyConfigScope anomalyConfigScope) {
    switch (anomalyConfigScope.getScopeCase()) {
      case ENVIRONMENT_SCOPE:
        return Optional.of(getRuleScope(anomalyConfigScope.getEnvironmentScope()));
      case SERVICE_SCOPE:
        return getRuleScope(anomalyConfigScope.getServiceScope());
      case API_SCOPE:
        return getRuleScope(anomalyConfigScope.getApiScope().getServiceScope());
      case PARAM_SCOPE:
        AnomalyParamScope paramScope = anomalyConfigScope.getParamScope();
        if (paramScope.hasApiScope()) {
          return getRuleScope(paramScope.getApiScope().getServiceScope());
        }
        if (paramScope.hasScope()) {
          AnomalyParamInfoScope paramInfoScope = paramScope.getScope();
          switch (paramInfoScope.getScopeCase()) {
            case SERVICE_SCOPE:
              return getRuleScope(paramInfoScope.getServiceScope());
            case API_SCOPE:
              return getRuleScope(paramInfoScope.getApiScope().getServiceScope());
            default:
              return Optional.empty();
          }
        }
      default:
        return Optional.empty();
    }
  }

  private DetectionExclusionRuleScope getRuleScope(AnomalyEnvironmentScope environmentScope) {
    return DetectionExclusionRuleScope.newBuilder()
        .setEnvironmentScope(
            EnvironmentScope.newBuilder().addEnvironmentIds(environmentScope.getEnvironmentId()))
        .build();
  }

  private Optional<DetectionExclusionRuleScope> getRuleScope(AnomalyServiceScope serviceScope) {
    return serviceScope.hasEnvironmentScope()
        ? Optional.of(getRuleScope(serviceScope.getEnvironmentScope()))
        : Optional.empty();
  }

  private Optional<ScopeCondition> getScopeCondition(AnomalyConfigScope anomalyConfigScope) {
    switch (anomalyConfigScope.getScopeCase()) {
      case SERVICE_SCOPE:
        return Optional.of(getScopeCondition(anomalyConfigScope.getServiceScope()));
      case API_SCOPE:
        return Optional.of(getScopeCondition(anomalyConfigScope.getApiScope()));
      case PARAM_SCOPE:
        AnomalyParamScope paramScope = anomalyConfigScope.getParamScope();
        if (paramScope.hasApiScope()) {
          return Optional.of(getScopeCondition(paramScope.getApiScope()));
        }
        if (paramScope.hasScope()) {
          AnomalyParamInfoScope paramInfoScope = paramScope.getScope();
          switch (paramInfoScope.getScopeCase()) {
            case SERVICE_SCOPE:
              return Optional.of(getScopeCondition(paramInfoScope.getServiceScope()));
            case API_SCOPE:
              return Optional.of(getScopeCondition(paramInfoScope.getApiScope()));
            default:
              return Optional.empty();
          }
        }
      default:
        return Optional.empty();
    }
  }

  private ScopeCondition getScopeCondition(AnomalyServiceScope serviceScope) {
    return ScopeCondition.newBuilder()
        .setEntityScope(
            EntityScope.newBuilder()
                .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                .addEntityIds(serviceScope.getId()))
        .build();
  }

  private ScopeCondition getScopeCondition(AnomalyApiScope apiScope) {
    return ScopeCondition.newBuilder()
        .setEntityScope(
            EntityScope.newBuilder()
                .setEntityType(EntityType.ENTITY_TYPE_API)
                .addEntityIds(apiScope.getId()))
        .build();
  }

  private AnomalousAttributeCondition getAnomalousAttributeCondition(
      AnomalyParamScope anomalyParamScope) {
    MatchCondition.Builder matchConditionBuilder = MatchCondition.newBuilder();
    AnomalyParamInfo paramInfo = anomalyParamScope.getParamInfo();

    if (paramInfo.hasParamName()) {
      matchConditionBuilder
          .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
          .setValue(Value.newBuilder().setStringValue(paramInfo.getParamName()));
    } else if (paramInfo.hasParamRegex()) {
      matchConditionBuilder
          .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
          .setValue(Value.newBuilder().setStringValue(paramInfo.getParamRegex()));
    } else {
      matchConditionBuilder
          .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
          .setValue(Value.newBuilder().setStringValue(anomalyParamScope.getParamName()));
    }

    return AnomalousAttributeCondition.newBuilder()
        .setKeyMatchCondition(matchConditionBuilder)
        .build();
  }

  private EventCondition getEventCondition(EventExclusionInfo eventExclusionInfo) {
    if (eventExclusionInfo
        .getEventExclusionType()
        .equals(EventExclusionType.EVENT_EXCLUSION_TYPE_ALL_EVENTS)) {
      return ALL_SYSTEM_DEFINED_EVENTS_CONDITION;
    }

    AnomalyEventFamily anomalyEventFamily = eventExclusionInfo.getAnomalyEventFamily();
    switch (anomalyEventFamily) {
      case ANOMALY_EVENT_FAMILY_CUSTOM_SIGNATURE:
        return EventCondition.newBuilder()
            .addCustomRuleEvents(
                CustomRuleEvent.newBuilder()
                    .setRuleFamily(CUSTOM_RULE_FAMILY_SIGNATURE)
                    .setRuleId(eventExclusionInfo.getEventTypeId()))
            .build();
      case ANOMALY_EVENT_FAMILY_API_DEF:
      case ANOMALY_EVENT_FAMILY_SESSION:
      case ANOMALY_EVENT_FAMILY_MODSEC:
        SystemDefinedEvent.Builder eventBuilder =
            SystemDefinedEvent.newBuilder()
                .setEventFamily(systemDefinedEventFamilyMap.get(anomalyEventFamily));
        switch (eventExclusionInfo.getEventExclusionType()) {
          case EVENT_EXCLUSION_TYPE_EVENT_SUBTYPE:
            eventBuilder.setEventSubTypeId(eventExclusionInfo.getEventTypeId());
            break;
          case EVENT_EXCLUSION_TYPE_EVENT_TYPE:
            if (anomalyEventFamily.equals(ANOMALY_EVENT_FAMILY_MODSEC)) {
              eventBuilder.setEventTypeId(
                  getParentModsecRuleId(eventExclusionInfo.getEventTypeId()));
            } else {
              eventBuilder.setEventTypeId(eventExclusionInfo.getEventTypeId());
            }
            break;
          default:
            throw new IllegalStateException(
                String.format(
                    "Invalid exclusion type in Event Exclusion Info: {%s}", eventExclusionInfo));
        }
        return EventCondition.newBuilder().addSystemDefinedEvents(eventBuilder).build();
      default:
        throw new IllegalStateException(
            String.format("Event Exclusion Info cannot be converted for {%s}", eventExclusionInfo));
    }
  }

  private String getParentModsecRuleId(String modsecRuleId) {
    // crs_### is the parent rule-id
    return modsecRuleId.substring(0, 7);
  }
}
