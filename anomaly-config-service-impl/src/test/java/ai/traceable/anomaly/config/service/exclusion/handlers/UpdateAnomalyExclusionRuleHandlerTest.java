package ai.traceable.anomaly.config.service.exclusion.handlers;

import static ai.traceable.anomaly.config.service.exclusion.ExclusionTestUtils.getAllConfigs;
import static ai.traceable.anomaly.config.service.exclusion.ExclusionTestUtils.mockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.anomaly.config.service.exclusion.ExclusionTestUtils;
import ai.traceable.anomaly.config.service.exclusion.converters.AnomalyExclusionRuleConfigConverter;
import ai.traceable.anomaly.config.service.exclusion.utils.UuidGenerator;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigScope;
import ai.traceable.anomaly.config.service.v1.AnomalyConfigStatus;
import ai.traceable.anomaly.config.service.v1.AnomalyCustomerScope;
import ai.traceable.anomaly.config.service.v1.AnomalyEventFamily;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActor;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyActorExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleConfig;
import ai.traceable.anomaly.config.service.v1.exclusion.AnomalyExclusionRuleData;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionInfo;
import ai.traceable.anomaly.config.service.v1.exclusion.EventExclusionType;
import ai.traceable.anomaly.config.service.v1.exclusion.UpdateAnomalyExclusionRuleRequest;
import com.google.protobuf.Value;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UpdateAnomalyExclusionRuleHandlerTest {
  private ConfigServiceHandler configServiceHandler;
  private MockGenericConfigService mockConfigService;
  private ConfigServiceBlockingStub configServiceBlockingStub;
  private UpdateAnomalyExclusionRuleHandler updateAnomalyExclusionRuleHandler;
  private AnomalyExclusionRuleConfigConverter ruleConfigConverter;
  private UuidGenerator uuidGenerator;

  @BeforeEach
  void setUp() {
    mockConfigService =
        new MockGenericConfigService().mockDelete().mockUpsert().mockGetAll().mockGet();
    mockConfigService.start();
    configServiceBlockingStub = ConfigServiceGrpc.newBlockingStub(mockConfigService.channel());
    configServiceHandler = new ConfigServiceHandler(configServiceBlockingStub);
    ruleConfigConverter = mock(AnomalyExclusionRuleConfigConverter.class);
    uuidGenerator = mock(UuidGenerator.class);
    updateAnomalyExclusionRuleHandler =
        new UpdateAnomalyExclusionRuleHandler(configServiceHandler, ruleConfigConverter);
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

    Value mockConfig = mockConfig("rule_id", "rule_name");
    ExclusionTestUtils.upsertConfigs(Map.of("rule_id", mockConfig), configServiceBlockingStub);

    AnomalyExclusionRuleConfig mockUpdatedConfig =
        exclusionRuleConfig.toBuilder()
            .setRuleData(
                ruleData.toBuilder().setName("new_name").setDescription("new_description").build())
            .setConfigStatus(AnomalyConfigStatus.newBuilder().setDisabled(true).build())
            .build();

    when(ruleConfigConverter.convert(mockUpdatedConfig)).thenReturn(mockConfig);
    when(ruleConfigConverter.convert(mockConfig)).thenReturn(mockUpdatedConfig);
    when(uuidGenerator.generateId(exclusionRuleConfig.getRuleData())).thenReturn("rule_id");

    UpdateAnomalyExclusionRuleRequest updateRequest =
        UpdateAnomalyExclusionRuleRequest.newBuilder()
            .setRuleId("rule_id")
            .setName("new_name")
            .setDescription("new_description")
            .setConfigStatus(AnomalyConfigStatus.newBuilder().setDisabled(true).build())
            .build();

    updateAnomalyExclusionRuleHandler.updateRule(updateRequest);
    List<Value> configs = getAllConfigs(configServiceBlockingStub);
    assertEquals(1, configs.size());
    assertEquals(mockConfig, configs.get(0));
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
        StatusRuntimeException.class,
        () -> updateAnomalyExclusionRuleHandler.updateRule(updateRequest));
  }
}
