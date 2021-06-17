package ai.traceable.anomaly.config.service.exclusion.handlers;

import static ai.traceable.anomaly.config.service.exclusion.ExclusionTestUtils.mockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.exclusion.ExclusionTestUtils;
import ai.traceable.anomaly.config.service.exclusion.converters.AnomalyExclusionRuleConfigConverter;
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
import com.google.protobuf.Value;
import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class CreateAnomalyExclusionRuleHandlerTest {
  private ConfigServiceHandler configServiceHandler;
  private MockGenericConfigService mockConfigService;
  private ConfigServiceBlockingStub configServiceBlockingStub;
  private CreateAnomalyExclusionRuleHandler createAnomalyExclusionRuleHandler;
  private AnomalyExclusionRuleConfigConverter ruleConfigConverter;
  private ModsecRuleUtils modsecRuleUtils;
  private UuidGenerator uuidGenerator;

  @BeforeEach
  void setUp() {
    mockConfigService =
        new MockGenericConfigService().mockDelete().mockUpsert().mockGetAll().mockGet();
    mockConfigService.start();
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    configServiceHandler = new ConfigServiceHandler(configServiceBlockingStub);
    ruleConfigConverter = mock(AnomalyExclusionRuleConfigConverter.class);
    modsecRuleUtils = mock(ModsecRuleUtils.class);
    uuidGenerator = mock(UuidGenerator.class);
    createAnomalyExclusionRuleHandler =
        new CreateAnomalyExclusionRuleHandler(
            configServiceHandler, ruleConfigConverter, modsecRuleUtils, uuidGenerator);
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

    Value mockConfig = mockConfig("rule_id", "rule_name");

    when(ruleConfigConverter.convert(exclusionRuleConfig)).thenReturn(mockConfig);
    when(ruleConfigConverter.convert(mockConfig)).thenReturn(exclusionRuleConfig);
    when(uuidGenerator.generateId(exclusionRuleConfig.getRuleData())).thenReturn("rule_id");

    CreateAnomalyExclusionRuleRequest createRequest =
        CreateAnomalyExclusionRuleRequest.newBuilder().setRuleData(ruleData).build();

    createAnomalyExclusionRuleHandler.createRule(createRequest);
    List<Value> configs = ExclusionTestUtils.getAllConfigs(configServiceBlockingStub);
    assertEquals(1, configs.size());
    assertEquals(mockConfig, configs.get(0));
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

    Value mockConfig = mockConfig("rule_id", "rule_name");

    when(ruleConfigConverter.convert(exclusionRuleConfig)).thenReturn(mockConfig);
    when(ruleConfigConverter.convert(mockConfig)).thenReturn(exclusionRuleConfig);
    when(uuidGenerator.generateId(exclusionRuleConfig.getRuleData())).thenReturn("rule_id");
    when(modsecRuleUtils.getModsecParentRuleId("crs_111000")).thenReturn("crs_111");

    CreateAnomalyExclusionRuleRequest createRequest =
        CreateAnomalyExclusionRuleRequest.newBuilder().setRuleData(ruleData).build();

    createAnomalyExclusionRuleHandler.createRule(createRequest);
    List<Value> configs = ExclusionTestUtils.getAllConfigs(configServiceBlockingStub);
    assertEquals(1, configs.size());
    assertEquals(mockConfig, configs.get(0));
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

    Value mockConfig = mockConfig("rule_id", "rule_name");

    AnomalyExclusionRuleConfig mockEnrichedConfig =
        exclusionRuleConfig.toBuilder()
            .setRuleData(
                ruleData.toBuilder().setEventExclusionInfo(mockEnrichedExclusionInfo).build())
            .build();
    when(ruleConfigConverter.convert(mockEnrichedConfig)).thenReturn(mockConfig);
    when(ruleConfigConverter.convert(mockConfig)).thenReturn(mockEnrichedConfig);
    when(uuidGenerator.generateId(exclusionRuleConfig.getRuleData())).thenReturn("rule_id");
    when(modsecRuleUtils.getModsecParentRuleId("crs_111000")).thenReturn("crs_111");
    CreateAnomalyExclusionRuleRequest createRequest =
        CreateAnomalyExclusionRuleRequest.newBuilder().setRuleData(ruleData).build();

    createAnomalyExclusionRuleHandler.createRule(createRequest);
    ArgumentCaptor<AnomalyExclusionRuleConfig> configArgumentCaptor =
        ArgumentCaptor.forClass(AnomalyExclusionRuleConfig.class);
    verify(ruleConfigConverter).convert(configArgumentCaptor.capture());
    assertEquals(
        mockEnrichedExclusionInfo,
        configArgumentCaptor.getValue().getRuleData().getEventExclusionInfo());
    List<Value> configs = ExclusionTestUtils.getAllConfigs(configServiceBlockingStub);
    assertEquals(1, configs.size());
    assertEquals(mockConfig, configs.get(0));
  }
}
