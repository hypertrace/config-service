package ai.traceable.anomaly.config.service.exclusion.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeUtils;
import ai.traceable.anomaly.config.service.exclusion.utils.FilterUtils;
import ai.traceable.anomaly.config.service.exclusion.utils.UuidGenerator;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActor;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActorExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;
import ai.traceable.anomaly.config.service.v1.exclusion.CreateAnomalyExclusionRuleRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionType;
import ai.traceable.anomaly.config.service.v1.exclusion.GetAnomalyExclusionRulesRequest;
import ai.traceable.anomaly.config.service.v1.exclusion.UpdateAnomalyExclusionRuleRequest;
import ai.traceable.modsecurity.utils.ModsecRuleUtils;
import java.util.List;
import java.util.NoSuchElementException;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UpdateAnomalyExclusionRuleHandlerTest {
  private ConfigServiceHandler configServiceHandler;
  private MockGenericConfigService mockConfigService;
  private ConfigServiceBlockingStub configServiceBlockingStub;
  private GetAnomalyExclusionRuleHandler getAnomalyExclusionRuleHandler;
  private CreateAnomalyExclusionRuleHandler createAnomalyExclusionRuleHandler;
  private UpdateAnomalyExclusionRuleHandler updateAnomalyExclusionRuleHandler;
  private UuidGenerator uuidGenerator;
  private ModsecRuleUtils modsecRuleUtils;
  private RequestContext requestContext;
  private AnomalyConfigScopeUtils scopeMatcher;
  private FilterUtils filterUtils;

  @BeforeEach
  void setUp() {
    mockConfigService =
        new MockGenericConfigService().mockDelete().mockUpsert().mockGetAll().mockGet();
    mockConfigService.start();
    requestContext = RequestContext.forTenantId("tenant_id");
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    configServiceHandler =
        new ConfigServiceHandler(
            new AnomalyExclusionRuleConfigStore(
                configServiceBlockingStub, configChangeEventGenerator));
    uuidGenerator = mock(UuidGenerator.class);
    updateAnomalyExclusionRuleHandler = new UpdateAnomalyExclusionRuleHandler(configServiceHandler);
    scopeMatcher = mock(AnomalyConfigScopeUtils.class);
    modsecRuleUtils = mock(ModsecRuleUtils.class);
    filterUtils = mock(FilterUtils.class);
    when(scopeMatcher.isParentScope(any(AnomalyConfigScope.class), any(AnomalyConfigScope.class)))
        .thenReturn(true);
    when(filterUtils.filterRuleIds(any(), anySet())).thenReturn(true);
    when(filterUtils.filterAnomalyActorIds(any(), anySet())).thenReturn(true);
    when(filterUtils.filterEventFamilies(any(), anyList())).thenReturn(true);
    when(filterUtils.filterEventIds(any(), anyList())).thenReturn(true);
    getAnomalyExclusionRuleHandler =
        new GetAnomalyExclusionRuleHandler(configServiceHandler, scopeMatcher, filterUtils);
    createAnomalyExclusionRuleHandler =
        new CreateAnomalyExclusionRuleHandler(configServiceHandler, modsecRuleUtils, uuidGenerator);
  }

  @AfterEach
  void teardown() {
    mockConfigService.shutdown();
  }

  @Test
  void testUpdate() {
    AnomalyExclusionRuleData ruleData =
        AnomalyExclusionRuleData.newBuilder()
            .setName("old_name")
            .setDescription("old_description")
            .setEventExclusionInfo(
                EventExclusionInfo.newBuilder()
                    .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE)
                    .setEventTypeId("payload")
                    .setEventTypeName("payload anomaly")
                    .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF)
                    .build())
            .setAnomalyConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setCustomerScope(AnomalyCustomerScope.newBuilder().getDefaultInstanceForType())
                    .build())
            .setAnomalyActorExclusionInfo(
                AnomalyActorExclusionInfo.newBuilder()
                    .setAnomalyActor(AnomalyActor.newBuilder().setId("actor_id"))
                    .build())
            .build();

    AnomalyExclusionRuleConfig exclusionRuleConfig =
        AnomalyExclusionRuleConfig.newBuilder()
            .setId("rule_id")
            .setRuleData(ruleData)
            .setConfigStatus(AnomalyConfigStatus.newBuilder().getDefaultInstanceForType())
            .build();

    when(uuidGenerator.generateId(exclusionRuleConfig.getRuleData())).thenReturn("rule_id");

    createAnomalyExclusionRuleHandler.createRule(
        CreateAnomalyExclusionRuleRequest.newBuilder().setRuleData(ruleData).build(),
        requestContext);
    AnomalyExclusionRuleConfig mockUpdatedConfig =
        exclusionRuleConfig.toBuilder()
            .setRuleData(
                ruleData.toBuilder().setName("new_name").setDescription("new_description").build())
            .setConfigStatus(AnomalyConfigStatus.newBuilder().setDisabled(true).build())
            .build();

    UpdateAnomalyExclusionRuleRequest updateRequest =
        UpdateAnomalyExclusionRuleRequest.newBuilder()
            .setRuleId("rule_id")
            .setName("new_name")
            .setDescription("new_description")
            .setConfigStatus(AnomalyConfigStatus.newBuilder().setDisabled(true).build())
            .build();

    updateAnomalyExclusionRuleHandler.updateRule(updateRequest, requestContext);
    List<AnomalyExclusionRuleConfig> configs =
        getAnomalyExclusionRuleHandler
            .getRules(GetAnomalyExclusionRulesRequest.newBuilder().build(), requestContext)
            .getConfigsList();
    assertEquals(1, configs.size());
    assertEquals(mockUpdatedConfig, configs.get(0));
  }

  @Test
  void testUpdate_configNotPresent() {
    AnomalyExclusionRuleData ruleData =
        AnomalyExclusionRuleData.newBuilder()
            .setName("old_name")
            .setDescription("old_description")
            .setEventExclusionInfo(
                EventExclusionInfo.newBuilder()
                    .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE)
                    .setEventTypeId("payload")
                    .setEventTypeName("payload anomaly")
                    .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_API_DEF)
                    .build())
            .setAnomalyConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setCustomerScope(AnomalyCustomerScope.newBuilder().getDefaultInstanceForType())
                    .build())
            .setAnomalyActorExclusionInfo(
                AnomalyActorExclusionInfo.newBuilder()
                    .setAnomalyActor(AnomalyActor.newBuilder().setId("actor_id"))
                    .build())
            .build();
    when(uuidGenerator.generateId(ruleData)).thenReturn("rule_id");
    UpdateAnomalyExclusionRuleRequest updateRequest =
        UpdateAnomalyExclusionRuleRequest.newBuilder()
            .setRuleId("rule_id")
            .setName("new_name")
            .setDescription("new_description")
            .setConfigStatus(AnomalyConfigStatus.newBuilder().setDisabled(true).build())
            .build();

    Assertions.assertThrows(
        NoSuchElementException.class,
        () -> updateAnomalyExclusionRuleHandler.updateRule(updateRequest, requestContext));
  }
}
