package ai.traceable.anomaly.config.service.exclusion.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.common.AnomalyConfigScopeMatcher;
import ai.traceable.anomaly.config.service.exclusion.utils.FilterUtils;
import ai.traceable.anomaly.config.service.exclusion.utils.UuidGenerator;
import ai.traceable.anomaly.config.service.registry.modsec.ModsecRuleUtils;
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
import java.util.List;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CreateAnomalyExclusionRuleHandlerTest {
  private ConfigServiceHandler configServiceHandler;
  private MockGenericConfigService mockConfigService;
  private ConfigServiceBlockingStub configServiceBlockingStub;
  private CreateAnomalyExclusionRuleHandler createAnomalyExclusionRuleHandler;
  private ModsecRuleUtils modsecRuleUtils;
  private UuidGenerator uuidGenerator;
  private RequestContext requestContext;
  private AnomalyConfigScopeMatcher scopeMatcher;
  private FilterUtils filterUtils;
  private GetAnomalyExclusionRuleHandler getAnomalyExclusionRuleHandler;

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
    modsecRuleUtils = mock(ModsecRuleUtils.class);
    uuidGenerator = mock(UuidGenerator.class);
    scopeMatcher = mock(AnomalyConfigScopeMatcher.class);
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
  void createRule() {
    AnomalyExclusionRuleData ruleData =
        AnomalyExclusionRuleData.newBuilder()
            .setName("rule_name")
            .setDescription("description")
            .setEventExclusionInfo(
                EventExclusionInfo.newBuilder()
                    .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE)
                    .setEventTypeId("bola")
                    .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_SESSION)
                    .build())
            .setAnomalyConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setCustomerScope(AnomalyCustomerScope.newBuilder().getDefaultInstanceForType())
                    .build())
            .setAnomalyActorExclusionInfo(
                AnomalyActorExclusionInfo.newBuilder()
                    .setAnomalyActor(AnomalyActor.newBuilder().setId("actor_id").build())
                    .build())
            .build();

    AnomalyExclusionRuleConfig exclusionRuleConfig =
        AnomalyExclusionRuleConfig.newBuilder()
            .setId("rule_id")
            .setRuleData(ruleData)
            .setConfigStatus(AnomalyConfigStatus.newBuilder().getDefaultInstanceForType())
            .build();

    when(uuidGenerator.generateId(exclusionRuleConfig.getRuleData())).thenReturn("rule_id");

    CreateAnomalyExclusionRuleRequest createRequest =
        CreateAnomalyExclusionRuleRequest.newBuilder().setRuleData(ruleData).build();

    createAnomalyExclusionRuleHandler.createRule(createRequest, requestContext);
    List<AnomalyExclusionRuleConfig> configs =
        getAnomalyExclusionRuleHandler
            .getRules(GetAnomalyExclusionRulesRequest.newBuilder().build(), requestContext)
            .getConfigsList();
    assertEquals(1, configs.size());
    assertEquals(exclusionRuleConfig, configs.get(0));
  }

  @Test
  void createRule_modSec_exclusionType_eventSubType() {
    AnomalyExclusionRuleData ruleData =
        AnomalyExclusionRuleData.newBuilder()
            .setName("rule_name")
            .setDescription("description")
            .setEventExclusionInfo(
                EventExclusionInfo.newBuilder()
                    .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_SUBTYPE)
                    .setEventTypeId("crs_111000")
                    .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
                    .build())
            .setAnomalyConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setCustomerScope(AnomalyCustomerScope.newBuilder().getDefaultInstanceForType())
                    .build())
            .setAnomalyActorExclusionInfo(
                AnomalyActorExclusionInfo.newBuilder()
                    .setAnomalyActor(AnomalyActor.newBuilder().setId("actor_id").build())
                    .build())
            .build();

    AnomalyExclusionRuleConfig exclusionRuleConfig =
        AnomalyExclusionRuleConfig.newBuilder()
            .setId("rule_id")
            .setRuleData(ruleData)
            .setConfigStatus(AnomalyConfigStatus.newBuilder().getDefaultInstanceForType())
            .build();

    when(uuidGenerator.generateId(exclusionRuleConfig.getRuleData())).thenReturn("rule_id");
    when(modsecRuleUtils.getModsecParentRuleId("crs_111000")).thenReturn("crs_111");

    CreateAnomalyExclusionRuleRequest createRequest =
        CreateAnomalyExclusionRuleRequest.newBuilder().setRuleData(ruleData).build();

    createAnomalyExclusionRuleHandler.createRule(createRequest, requestContext);
    List<AnomalyExclusionRuleConfig> configs =
        getAnomalyExclusionRuleHandler
            .getRules(GetAnomalyExclusionRulesRequest.newBuilder().build(), requestContext)
            .getConfigsList();

    assertEquals(1, configs.size());
    assertEquals(exclusionRuleConfig, configs.get(0));
  }

  @Test
  void createRuleModSec_eventExclusionType_RuleType() {
    AnomalyExclusionRuleData ruleData =
        AnomalyExclusionRuleData.newBuilder()
            .setName("rule_name")
            .setDescription("description")
            .setEventExclusionInfo(
                EventExclusionInfo.newBuilder()
                    .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE)
                    .setEventTypeId("crs_111000")
                    .setEventTypeName("conditional sql")
                    .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
                    .build())
            .setAnomalyConfigScope(
                AnomalyConfigScope.newBuilder()
                    .setCustomerScope(AnomalyCustomerScope.newBuilder().getDefaultInstanceForType())
                    .build())
            .setAnomalyActorExclusionInfo(
                AnomalyActorExclusionInfo.newBuilder()
                    .setAnomalyActor(AnomalyActor.newBuilder().setId("actor_id").build())
                    .build())
            .build();

    AnomalyExclusionRuleConfig exclusionRuleConfig =
        AnomalyExclusionRuleConfig.newBuilder()
            .setId("rule_id")
            .setRuleData(ruleData)
            .setConfigStatus(AnomalyConfigStatus.newBuilder().getDefaultInstanceForType())
            .build();

    // Modsec event_type_id should be change to prefix of event_rule_id.
    EventExclusionInfo mockEnrichedExclusionInfo =
        EventExclusionInfo.newBuilder()
            .setEventExclusionType(EventExclusionType.EVENT_EXCLUSION_TYPE_EVENT_TYPE)
            .setEventTypeId("crs_111")
            .setEventTypeName("conditional sql")
            .setAnomalyEventFamily(AnomalyEventFamily.ANOMALY_EVENT_FAMILY_MODSEC)
            .build();

    AnomalyExclusionRuleConfig mockEnrichedConfig =
        exclusionRuleConfig.toBuilder()
            .setRuleData(
                ruleData.toBuilder().setEventExclusionInfo(mockEnrichedExclusionInfo).build())
            .build();
    when(uuidGenerator.generateId(exclusionRuleConfig.getRuleData())).thenReturn("rule_id");
    when(modsecRuleUtils.getModsecParentRuleId("crs_111000")).thenReturn("crs_111");
    CreateAnomalyExclusionRuleRequest createRequest =
        CreateAnomalyExclusionRuleRequest.newBuilder().setRuleData(ruleData).build();

    createAnomalyExclusionRuleHandler.createRule(createRequest, requestContext);
    List<AnomalyExclusionRuleConfig> configs =
        getAnomalyExclusionRuleHandler
            .getRules(GetAnomalyExclusionRulesRequest.newBuilder().build(), requestContext)
            .getConfigsList();
    assertEquals(1, configs.size());
    assertEquals(mockEnrichedConfig, configs.get(0));
  }
}
