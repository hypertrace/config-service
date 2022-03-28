package ai.traceable.data.exfiltration.config.service.detection.rule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.exfiltration.config.service.v1.CreateDataExfiltrationDetectionRuleRequest;
import ai.traceable.data.exfiltration.config.service.v1.DataExfiltrationDetectionRuleConfig;
import ai.traceable.data.exfiltration.config.service.v1.DataExfiltrationDetectionRuleConfigServiceGrpc;
import ai.traceable.data.exfiltration.config.service.v1.DataExfiltrationDetectionRuleConfigServiceGrpc.DataExfiltrationDetectionRuleConfigServiceBlockingStub;
import ai.traceable.data.exfiltration.config.service.v1.DataExfiltrationDetectionRuleData;
import ai.traceable.data.exfiltration.config.service.v1.DataScope;
import ai.traceable.data.exfiltration.config.service.v1.DeleteDataExfiltrationDetectionRuleRequest;
import ai.traceable.data.exfiltration.config.service.v1.DetectionConfigStatus;
import ai.traceable.data.exfiltration.config.service.v1.EntityScope;
import ai.traceable.data.exfiltration.config.service.v1.EnvironmentScope;
import ai.traceable.data.exfiltration.config.service.v1.GetDataExfiltrationDetectionRulesRequest;
import ai.traceable.data.exfiltration.config.service.v1.Scope;
import ai.traceable.data.exfiltration.config.service.v1.UpdateDataExfiltrationDetectionRuleRequest;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DataExfiltrationDetectionRulesConfigServiceImplTest {
  MockGenericConfigService mockGenericConfigService;
  DataExfiltrationDetectionRuleConfigServiceBlockingStub
      dataExfiltrationDetectionRuleConfigServiceBlockingStub;

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockGenericConfigService
        .addService(
            new DataExfiltrationDetectionRulesConfigServiceImpl(
                new DataExfiltrationDetectionRulesStore(
                    this.mockGenericConfigService.channel(), configChangeEventGenerator),
                new DataExfiltrationDetectionRulesRequestValidator(),
                new UuidGenerator()))
        .start();
    dataExfiltrationDetectionRuleConfigServiceBlockingStub =
        DataExfiltrationDetectionRuleConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void testCreateReadUpdateDelete() {
    DataExfiltrationDetectionRuleData data = getSampleData();

    // CREATE
    DataExfiltrationDetectionRuleConfig dataExfiltrationDetectionRuleConfig =
        dataExfiltrationDetectionRuleConfigServiceBlockingStub
            .createDataExfiltrationDetectionRule(
                CreateDataExfiltrationDetectionRuleRequest.newBuilder().setRuleData(data).build())
            .getConfig();
    assertEquals(data, dataExfiltrationDetectionRuleConfig.getRuleData());

    // UPDATE
    DataExfiltrationDetectionRuleData updatedData = getSampleData();
    DataExfiltrationDetectionRuleConfig updatedConfig =
        dataExfiltrationDetectionRuleConfigServiceBlockingStub
            .updateDataExfiltrationDetectionRule(
                UpdateDataExfiltrationDetectionRuleRequest.newBuilder()
                    .setRuleConfig(
                        DataExfiltrationDetectionRuleConfig.newBuilder(
                                dataExfiltrationDetectionRuleConfig)
                            .setRuleData(updatedData))
                    .build())
            .getConfig();

    // id should match
    assertEquals(dataExfiltrationDetectionRuleConfig.getId(), updatedConfig.getId());
    assertEquals(updatedData, updatedConfig.getRuleData());

    // READ
    DataExfiltrationDetectionRuleConfig secondConfig =
        dataExfiltrationDetectionRuleConfigServiceBlockingStub
            .createDataExfiltrationDetectionRule(
                CreateDataExfiltrationDetectionRuleRequest.newBuilder()
                    .setRuleData(getSampleData())
                    .build())
            .getConfig();
    assertEquals(
        List.of(secondConfig, updatedConfig),
        dataExfiltrationDetectionRuleConfigServiceBlockingStub
            .getDataExfiltrationDetectionRules(
                GetDataExfiltrationDetectionRulesRequest.newBuilder().build())
            .getConfigsList());

    // DELETE
    dataExfiltrationDetectionRuleConfigServiceBlockingStub.deleteDataExfiltrationDetectionRule(
        DeleteDataExfiltrationDetectionRuleRequest.newBuilder()
            .setRuleId(dataExfiltrationDetectionRuleConfig.getId())
            .build());

    assertFalse(
        dataExfiltrationDetectionRuleConfigServiceBlockingStub
            .getDataExfiltrationDetectionRules(
                GetDataExfiltrationDetectionRulesRequest.newBuilder().build())
            .getConfigsList()
            .stream()
            .map(DataExfiltrationDetectionRuleConfig::getId)
            .collect(Collectors.toList())
            .contains(dataExfiltrationDetectionRuleConfig.getId()));

    assertEquals(
        1,
        dataExfiltrationDetectionRuleConfigServiceBlockingStub
            .getDataExfiltrationDetectionRules(
                GetDataExfiltrationDetectionRulesRequest.newBuilder().build())
            .getConfigsList()
            .size());
  }

  private DataExfiltrationDetectionRuleData getSampleData() {
    return DataExfiltrationDetectionRuleData.newBuilder()
        .setName(generateRandomString())
        .addAllDataScopes(
            List.of(
                DataScope.newBuilder().setDataSetId(generateRandomString()).build(),
                DataScope.newBuilder().setDataTypeId(generateRandomString()).build()))
        .addAllScopes(
            List.of(
                Scope.newBuilder().setApiScope(getEntitiyScopeFor(true)).build(),
                Scope.newBuilder().setApiScope(getEntitiyScopeFor(false)).build(),
                Scope.newBuilder().setServiceScope(getEntitiyScopeFor(true)).build(),
                Scope.newBuilder().setServiceScope(getEntitiyScopeFor(false)).build(),
                Scope.newBuilder()
                    .setEnvironmentScope(
                        EnvironmentScope.newBuilder().setId(generateRandomString()))
                    .build()))
        .setConfigStatus(DetectionConfigStatus.newBuilder().setDisabled(true))
        .build();
  }

  private String generateRandomString() {
    byte[] array = new byte[7]; // length is bounded by 7
    new Random().nextBytes(array);
    return new String(array, Charset.forName("UTF-8"));
  }

  private EntityScope getEntitiyScopeFor(boolean isLabelId) {
    return isLabelId
        ? EntityScope.newBuilder().setLabelId(generateRandomString()).build()
        : EntityScope.newBuilder().setEntityId(generateRandomString()).build();
  }
}
