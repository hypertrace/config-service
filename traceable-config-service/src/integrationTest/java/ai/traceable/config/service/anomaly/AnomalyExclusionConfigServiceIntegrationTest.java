package ai.traceable.config.service.anomaly;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.AnomalyServiceScope;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActor;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActorExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionConfigServiceGrpc;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;
import ai.traceable.anomaly.config.service.v1.exclusion.CreateAnomalyExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.DeleteAnomalyExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionType;
import ai.traceable.anomaly.config.service.v1.exclusion.GetAnomalyExclusionRulesRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.GetRulesFilter;
import ai.traceable.anomaly.config.service.v1.exclusion.UpdateAnomalyExclusionRuleRequest;
import ai.traceable.config.service.TraceableConfigServiceIntegrationTestBase;
import java.util.List;
import org.hypertrace.core.grpcutils.client.GrpcClientRequestContextUtil;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AnomalyExclusionConfigServiceIntegrationTest
    extends TraceableConfigServiceIntegrationTestBase {
  private static AnomalyExclusionConfigServiceGrpc.AnomalyExclusionConfigServiceBlockingStub
      anomalyExclusionConfigServiceStub;

  @BeforeAll
  static void init() {
    anomalyExclusionConfigServiceStub =
        AnomalyExclusionConfigServiceGrpc.newBlockingStub(channelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testGetAnomalyExclusionRules() {
    AnomalyExclusionRuleData ruleData =
        AnomalyExclusionRuleData.newBuilder()
            .setName("rule_name")
            .setDescription("description")
            .setEventExclusionInfo(
                EventExclusionInfo.newBuilder()
                    .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE)
                    .setEventTypeId("bola")
                    .setEventTypeName("Broken Object")
                    .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION)
                    .build())
            .setAnomalyConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setServiceScope(AnomalyServiceScope.newBuilder().setId("service_id").build())
                    .build())
            .setAnomalyActorExclusionInfo(
                AnomalyActorExclusionInfo.newBuilder()
                    .setAnomalyActor(AnomalyActor.newBuilder().setId("actor_id").build())
                    .build())
            .build();

    AnomalyExclusionRuleData otherRuleData =
        AnomalyExclusionRuleData.newBuilder()
            .setName("other_name")
            .setDescription("description")
            .setEventExclusionInfo(
                EventExclusionInfo.newBuilder()
                    .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_ALL_EVENTS)
                    .build())
            .setAnomalyConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setServiceScope(AnomalyServiceScope.newBuilder().setId("service_id").build())
                    .build())
            .setAnomalyActorExclusionInfo(
                AnomalyActorExclusionInfo.newBuilder()
                    .setAnomalyActor(AnomalyActor.newBuilder().setId("actor_id").build())
                    .build())
            .build();

    AnomalyExclusionRuleConfig createdRuleConfig =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                anomalyExclusionConfigServiceStub
                    .createAnomalyExclusionRule(
                        CreateAnomalyExclusionRuleRequest.newBuilder()
                            .setRuleData(ruleData)
                            .build())
                    .getConfig());

    AnomalyExclusionRuleConfig otherCreatedConfig =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                anomalyExclusionConfigServiceStub
                    .createAnomalyExclusionRule(
                        CreateAnomalyExclusionRuleRequest.newBuilder()
                            .setRuleData(otherRuleData)
                            .build())
                    .getConfig());

    List<AnomalyExclusionRuleConfig> getRulesConfigs =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    anomalyExclusionConfigServiceStub.getAnomalyExclusionRules(
                        GetAnomalyExclusionRulesRequest.newBuilder()
                            .setFilter(
                                GetRulesFilter.newBuilder()
                                    .addRuleIds(createdRuleConfig.getId())
                                    .build())
                            .build()))
            .getConfigsList();
    assertEquals(1, getRulesConfigs.size());
    assertEquals(
        createConfigFromRuleData(createdRuleConfig.getId(), ruleData), getRulesConfigs.get(0));

    List<AnomalyExclusionRuleConfig> allRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    anomalyExclusionConfigServiceStub.getAnomalyExclusionRules(
                        GetAnomalyExclusionRulesRequest.newBuilder()
                            .setFilter(GetRulesFilter.newBuilder().build())
                            .build()))
            .getConfigsList();
    assertEquals(2, allRules.size());
    assertEquals(createConfigFromRuleData(createdRuleConfig.getId(), ruleData), allRules.get(1));
    assertEquals(createConfigFromRuleData(allRules.get(0).getId(), otherRuleData), allRules.get(0));
  }

  @Test
  void testDeleteAnomalyExclusionRules() {
    AnomalyExclusionRuleData ruleData =
        AnomalyExclusionRuleData.newBuilder()
            .setName("rule_name")
            .setDescription("description")
            .setEventExclusionInfo(
                EventExclusionInfo.newBuilder()
                    .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE)
                    .setEventTypeId("bola")
                    .setEventTypeName("Broken Object")
                    .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION)
                    .build())
            .setAnomalyConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setServiceScope(AnomalyServiceScope.newBuilder().setId("service_id").build())
                    .build())
            .setAnomalyActorExclusionInfo(
                AnomalyActorExclusionInfo.newBuilder()
                    .setAnomalyActor(AnomalyActor.newBuilder().setId("actor_id").build())
                    .build())
            .build();

    AnomalyExclusionRuleData otherRuleData =
        AnomalyExclusionRuleData.newBuilder()
            .setName("other_name")
            .setDescription("description")
            .setEventExclusionInfo(
                EventExclusionInfo.newBuilder()
                    .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_ALL_EVENTS)
                    .build())
            .setAnomalyConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setServiceScope(AnomalyServiceScope.newBuilder().setId("service_id").build())
                    .build())
            .setAnomalyActorExclusionInfo(
                AnomalyActorExclusionInfo.newBuilder()
                    .setAnomalyActor(AnomalyActor.newBuilder().setId("actor_id").build())
                    .build())
            .build();

    AnomalyExclusionRuleConfig createdRuleConfig =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                anomalyExclusionConfigServiceStub
                    .createAnomalyExclusionRule(
                        CreateAnomalyExclusionRuleRequest.newBuilder()
                            .setRuleData(ruleData)
                            .build())
                    .getConfig());

    AnomalyExclusionRuleConfig otherCreatedConfig =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                anomalyExclusionConfigServiceStub
                    .createAnomalyExclusionRule(
                        CreateAnomalyExclusionRuleRequest.newBuilder()
                            .setRuleData(otherRuleData)
                            .build())
                    .getConfig());

    // call delete on  firstly created rule
    GrpcClientRequestContextUtil.executeInTenantContext(
        TENANT_ID,
        () ->
            anomalyExclusionConfigServiceStub.deleteAnomalyExclusionRule(
                DeleteAnomalyExclusionRuleRequest.newBuilder()
                    .setRuleId(createdRuleConfig.getId())
                    .build()));

    // get all rules
    List<AnomalyExclusionRuleConfig> allRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    anomalyExclusionConfigServiceStub.getAnomalyExclusionRules(
                        GetAnomalyExclusionRulesRequest.newBuilder()
                            .setFilter(GetRulesFilter.newBuilder().build())
                            .build()))
            .getConfigsList();
    // Assert only other rule remains
    assertEquals(1, allRules.size());
    assertEquals(otherCreatedConfig.getId(), allRules.get(0).getId());
  }

  @Test
  void testUpdateAnomalyExclusionRules() {
    AnomalyExclusionRuleData ruleData =
        AnomalyExclusionRuleData.newBuilder()
            .setName("rule_name")
            .setDescription("description")
            .setEventExclusionInfo(
                EventExclusionInfo.newBuilder()
                    .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE)
                    .setEventTypeId("bola")
                    .setEventTypeName("Broken Object")
                    .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION)
                    .build())
            .setAnomalyConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setServiceScope(AnomalyServiceScope.newBuilder().setId("service_id").build())
                    .build())
            .setAnomalyActorExclusionInfo(
                AnomalyActorExclusionInfo.newBuilder()
                    .setAnomalyActor(AnomalyActor.newBuilder().setId("actor_id").build())
                    .build())
            .build();

    AnomalyExclusionRuleData otherRuleData =
        AnomalyExclusionRuleData.newBuilder()
            .setName("other_name")
            .setDescription("description")
            .setEventExclusionInfo(
                EventExclusionInfo.newBuilder()
                    .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_ALL_EVENTS)
                    .build())
            .setAnomalyConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setServiceScope(AnomalyServiceScope.newBuilder().setId("service_id").build())
                    .build())
            .setAnomalyActorExclusionInfo(
                AnomalyActorExclusionInfo.newBuilder()
                    .setAnomalyActor(AnomalyActor.newBuilder().setId("actor_id").build())
                    .build())
            .build();

    AnomalyExclusionRuleConfig createdRuleConfig =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                anomalyExclusionConfigServiceStub
                    .createAnomalyExclusionRule(
                        CreateAnomalyExclusionRuleRequest.newBuilder()
                            .setRuleData(ruleData)
                            .build())
                    .getConfig());

    // call update
    AnomalyExclusionRuleConfig updatedConfig =
        GrpcClientRequestContextUtil.executeInTenantContext(
            TENANT_ID,
            () ->
                anomalyExclusionConfigServiceStub
                    .updateAnomalyExclusionRule(
                        UpdateAnomalyExclusionRuleRequest.newBuilder()
                            .setRuleId(createdRuleConfig.getId())
                            .setName("new_name")
                            .setDescription("new description")
                            .setConfigStatus(AnomalyConfigStatus.newBuilder().setDisabled(true))
                            .build())
                    .getConfig());

    // get all rules
    List<AnomalyExclusionRuleConfig> allRules =
        GrpcClientRequestContextUtil.executeInTenantContext(
                TENANT_ID,
                () ->
                    anomalyExclusionConfigServiceStub.getAnomalyExclusionRules(
                        GetAnomalyExclusionRulesRequest.newBuilder()
                            .setFilter(GetRulesFilter.newBuilder().build())
                            .build()))
            .getConfigsList();

    // Assert only other rule remains
    assertEquals(1, allRules.size());
    assertEquals("new_name", allRules.get(0).getRuleData().getName());
    assertEquals("new description", allRules.get(0).getRuleData().getDescription());
    assertEquals(
        AnomalyConfigStatus.newBuilder().setDisabled(true).build(),
        allRules.get(0).getConfigStatus());
  }

  private AnomalyExclusionRuleConfig createConfigFromRuleData(
      String ruleId, AnomalyExclusionRuleData ruleData) {
    return AnomalyExclusionRuleConfig.newBuilder()
        .setRuleData(ruleData)
        .setId(ruleId)
        .setConfigStatus(AnomalyConfigStatus.newBuilder().getDefaultInstanceForType())
        .build();
  }
}
