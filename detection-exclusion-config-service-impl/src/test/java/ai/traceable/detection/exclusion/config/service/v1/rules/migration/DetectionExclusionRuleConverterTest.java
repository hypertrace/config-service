package ai.traceable.detection.exclusion.config.service.v1.rules.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.v1.AnomalyApiScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyEnvironmentScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyParamInfo;
import ai.traceable.anomaly.config.service.v1.AnomalyParamInfoScope;
import ai.traceable.anomaly.config.service.v1.AnomalyParamScope;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActor;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActorExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionType;
import ai.traceable.detection.exclusion.config.service.v1.AnomalousAttributeCondition;
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
import org.junit.jupiter.api.Test;

class DetectionExclusionRuleConverterTest {
  private final DetectionExclusionRuleConverter ruleConverter =
      new DetectionExclusionRuleConverter();

  @Test
  void testConvertRule() {
    AnomalyServiceScope serviceScope =
        AnomalyServiceScope.newBuilder()
            .setId("service")
            .setEnvironmentScope(AnomalyEnvironmentScope.newBuilder().setEnvironmentId("env"))
            .build();
    AnomalyApiScope apiScope =
        AnomalyApiScope.newBuilder().setId("api").setServiceScope(serviceScope).build();

    AnomalyExclusionRuleConfig oldRule1 =
        AnomalyExclusionRuleConfig.newBuilder()
            .setId("id1")
            .setConfigStatus(AnomalyConfigStatus.newBuilder().setInternal(true))
            .setRuleData(
                AnomalyExclusionRuleData.newBuilder()
                    .setName("name1")
                    .setDescription("desc1")
                    .setEventExclusionInfo(
                        EventExclusionInfo.newBuilder()
                            .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
                            .setEventExclusionType(
                                EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE)
                            .setEventTypeId("crs_941112"))
                    .setAnomalyConfigScope(AnomalyConfigScope.newBuilder().setApiScope(apiScope))
                    .setSourceConfigScope(
                        AnomalyConfigScope.newBuilder()
                            .setParamScope(
                                AnomalyParamScope.newBuilder()
                                    .setParamInfo(
                                        AnomalyParamInfo.newBuilder().setParamName("param1")))))
            .build();

    AnomalyExclusionRuleConfig oldRule2 =
        AnomalyExclusionRuleConfig.newBuilder()
            .setId("id2")
            .setConfigStatus(AnomalyConfigStatus.newBuilder().setDisabled(true))
            .setRuleData(
                AnomalyExclusionRuleData.newBuilder()
                    .setName("name2")
                    .setDescription("desc2")
                    .setEventExclusionInfo(
                        EventExclusionInfo.newBuilder()
                            .setEventExclusionType(
                                EventExclusionType.EVENT_EXCLUSION_TYPE_ALL_EVENTS))
                    .setAnomalyActorExclusionInfo(
                        AnomalyActorExclusionInfo.newBuilder()
                            .setAnomalyActor(AnomalyActor.newBuilder().setId("actorEntityId")))
                    .setAnomalyConfigScope(
                        AnomalyConfigScope.newBuilder()
                            .setParamScope(
                                AnomalyParamScope.newBuilder()
                                    .setScope(
                                        AnomalyParamInfoScope.newBuilder()
                                            .setServiceScope(serviceScope))
                                    .setParamInfo(
                                        AnomalyParamInfo.newBuilder()
                                            .setParamRegex("param2\\..+"))))
                    .setSourceConfigScope(AnomalyConfigScope.newBuilder().setApiScope(apiScope)))
            .build();

    DetectionExclusionRuleScope ruleScope =
        DetectionExclusionRuleScope.newBuilder()
            .setEnvironmentScope(EnvironmentScope.newBuilder().addEnvironmentIds("env"))
            .build();

    DetectionExclusionRule expectedNewRule1 =
        DetectionExclusionRule.newBuilder()
            .setId("id1")
            .setRuleScope(ruleScope)
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("name1")
                    .setDescription("desc1")
                    .setRuleStatus(
                        DetectionExclusionRuleStatus.newBuilder()
                            .setGenerateInternalEvents(true)
                            .setHidden(true)
                            .setRuleCreationSource(RuleSource.RULE_SOURCE_OLD_API))
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setEventCondition(
                                EventCondition.newBuilder()
                                    .addSystemDefinedEvents(
                                        SystemDefinedEvent.newBuilder()
                                            .setEventFamily(
                                                SystemDefinedEventFamily
                                                    .SYSTEM_DEFINED_EVENT_FAMILY_MODSEC)
                                            .setEventTypeId("crs_941"))))
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setScopeCondition(
                                ScopeCondition.newBuilder()
                                    .setEntityScope(
                                        EntityScope.newBuilder()
                                            .setEntityType(EntityType.ENTITY_TYPE_API)
                                            .addEntityIds("api"))))
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setSourceAnomalousAttributeMatchCondition(
                                AnomalousAttributeCondition.newBuilder()
                                    .setKeyMatchCondition(
                                        MatchCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_EQUALS)
                                            .setValue(
                                                Value.newBuilder().setStringValue("param1"))))))
            .build();

    DetectionExclusionRule expectedNewRule2 =
        DetectionExclusionRule.newBuilder()
            .setId("id2")
            .setRuleScope(ruleScope)
            .setRuleInfo(
                DetectionExclusionRuleInfo.newBuilder()
                    .setName("name2")
                    .setDescription("desc2")
                    .setRuleStatus(
                        DetectionExclusionRuleStatus.newBuilder()
                            .setDisabled(true)
                            .setHidden(true)
                            .setGenerateInternalEvents(false)
                            .setRuleCreationSource(RuleSource.RULE_SOURCE_OLD_API))
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setEventCondition(
                                EventCondition.newBuilder()
                                    .addSystemDefinedEvents(
                                        SystemDefinedEvent.newBuilder()
                                            .setEventFamily(
                                                SystemDefinedEventFamily
                                                    .SYSTEM_DEFINED_EVENT_FAMILY_API_DEF))
                                    .addSystemDefinedEvents(
                                        SystemDefinedEvent.newBuilder()
                                            .setEventFamily(
                                                SystemDefinedEventFamily
                                                    .SYSTEM_DEFINED_EVENT_FAMILY_MODSEC))
                                    .addSystemDefinedEvents(
                                        SystemDefinedEvent.newBuilder()
                                            .setEventFamily(
                                                SystemDefinedEventFamily
                                                    .SYSTEM_DEFINED_EVENT_FAMILY_SESSION))))
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setScopeCondition(
                                ScopeCondition.newBuilder()
                                    .setEntityScope(
                                        EntityScope.newBuilder()
                                            .setEntityType(EntityType.ENTITY_TYPE_SERVICE)
                                            .addEntityIds("service"))))
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setAnomalousAttributeCondition(
                                AnomalousAttributeCondition.newBuilder()
                                    .setKeyMatchCondition(
                                        MatchCondition.newBuilder()
                                            .setOperator(MatchOperator.MATCH_OPERATOR_MATCHES_REGEX)
                                            .setValue(
                                                Value.newBuilder().setStringValue("param2\\..+")))))
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setSourceScopeCondition(
                                ScopeCondition.newBuilder()
                                    .setEntityScope(
                                        EntityScope.newBuilder()
                                            .setEntityType(EntityType.ENTITY_TYPE_API)
                                            .addEntityIds("api"))))
                    .addConditions(
                        DetectionExclusionCondition.newBuilder()
                            .setUserIdCondition(
                                UserIdCondition.newBuilder().addActorEntityIds("actorEntityId"))))
            .build();

    assertEquals(expectedNewRule1, ruleConverter.convertRule(oldRule1));
    assertEquals(expectedNewRule2, ruleConverter.convertRule(oldRule2));
  }
}
