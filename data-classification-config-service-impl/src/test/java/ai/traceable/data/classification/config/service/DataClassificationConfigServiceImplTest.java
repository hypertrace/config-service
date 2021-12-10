package ai.traceable.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import ai.traceable.data.classification.config.service.v1.CreateDataSetRequest;
import ai.traceable.data.classification.config.service.v1.CreateDataSetResponse;
import ai.traceable.data.classification.config.service.v1.CreateDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.CreateDataTypeResponse;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ApiScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Operator;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.data.classification.config.service.v1.DeleteDataSetRequest;
import ai.traceable.data.classification.config.service.v1.DeleteDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetResponse;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetsResponse;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import ai.traceable.data.classification.config.service.v1.UpdateDataSetRequest;
import ai.traceable.data.classification.config.service.v1.UpdateDataSetResponse;
import ai.traceable.data.classification.config.service.v1.UpdateDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.UpdateDataTypeResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DataClassificationConfigServiceImplTest {
  MockGenericConfigService mockGenericConfigService;
  DataClassificationConfigServiceBlockingStub dataClassificationConfigServiceBlockingStub;

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockGenericConfigService
        .addService(
            new DataClassificationConfigServiceImpl(
                new DataSetStore(genericStub, configChangeEventGenerator),
                new DataTypeStore(genericStub, configChangeEventGenerator),
                new DataSetConfigRequestValidator(),
                new DataTypeConfigRequestValidator()))
        .start();
    dataClassificationConfigServiceBlockingStub =
        DataClassificationConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void createDataTypeTest() {
    DataTypeRule dataTypeRule = createDataTypeRuleForTest("data-type-rule-1", "value-1");
    CreateDataTypeRequest request =
        CreateDataTypeRequest.newBuilder().setRule(dataTypeRule).build();
    CreateDataTypeResponse response =
        dataClassificationConfigServiceBlockingStub.createDataType(request);
    assertEquals(dataTypeRule, response.getDataType().getRule());
  }

  @Test
  void getDataTypestest() {
    DataTypeRule dataTypeRule1 = createDataTypeRuleForTest("data-type-rule-1", "value-1");
    DataTypeRule dataTypeRule2 = createDataTypeRuleForTest("data-type-rule-2", "value-2");
    CreateDataTypeRequest request =
        CreateDataTypeRequest.newBuilder().setRule(dataTypeRule1).build();
    dataClassificationConfigServiceBlockingStub.createDataType(request);
    request = CreateDataTypeRequest.newBuilder().setRule(dataTypeRule2).build();
    dataClassificationConfigServiceBlockingStub.createDataType(request);
    GetDataTypesRequest getRequest = GetDataTypesRequest.getDefaultInstance();
    GetDataTypesResponse response =
        dataClassificationConfigServiceBlockingStub.getDataTypes(getRequest);
    assertEquals(2, response.getDataTypesCount());
    assertEquals(
        List.of(dataTypeRule2, dataTypeRule1),
        response.getDataTypesList().stream().map(DataType::getRule).collect(Collectors.toList()));
  }

  @Test
  void updateDataTypeNotExisting() {
    DataTypeRule dataTypeRule = createDataTypeRuleForTest("data-type-rule-1", "value-1");
    UpdateDataTypeRequest request =
        UpdateDataTypeRequest.newBuilder().setId("random-id").setRule(dataTypeRule).build();
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              dataClassificationConfigServiceBlockingStub.updateDataType(request);
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void updateDataTypeTest() {
    DataTypeRule dataTypeRule1 = createDataTypeRuleForTest("data-type-rule-1", "value-1");
    DataTypeRule dataTypeRule2 = createDataTypeRuleForTest("data-type-rule-2", "value-2");
    CreateDataTypeResponse response =
        dataClassificationConfigServiceBlockingStub.createDataType(
            CreateDataTypeRequest.newBuilder().setRule(dataTypeRule1).build());
    String updateId = response.getDataType().getId();
    UpdateDataTypeRequest request =
        UpdateDataTypeRequest.newBuilder().setId(updateId).setRule(dataTypeRule2).build();
    UpdateDataTypeResponse updateresponse =
        dataClassificationConfigServiceBlockingStub.updateDataType(request);
    assertEquals(dataTypeRule2, updateresponse.getDataType().getRule());
  }

  @Test
  void deleteDataTypeNotExisting() {
    DeleteDataTypeRequest request = DeleteDataTypeRequest.newBuilder().setId("random-id").build();
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              dataClassificationConfigServiceBlockingStub.deleteDataType(request);
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void deleteDataTypeTest() {
    DataTypeRule dataTypeRule1 = createDataTypeRuleForTest("data-type-rule-1", "value-1");
    DataTypeRule dataTypeRule2 = createDataTypeRuleForTest("data-type-rule-2", "value-2");
    CreateDataTypeRequest request =
        CreateDataTypeRequest.newBuilder().setRule(dataTypeRule1).build();
    dataClassificationConfigServiceBlockingStub.createDataType(request);
    request = CreateDataTypeRequest.newBuilder().setRule(dataTypeRule2).build();
    CreateDataTypeResponse response =
        dataClassificationConfigServiceBlockingStub.createDataType(request);
    String deleteId = response.getDataType().getId();
    DeleteDataTypeRequest deleteRequest =
        DeleteDataTypeRequest.newBuilder().setId(deleteId).build();
    dataClassificationConfigServiceBlockingStub.deleteDataType(deleteRequest);
    GetDataTypesResponse getResponse =
        dataClassificationConfigServiceBlockingStub.getDataTypes(
            GetDataTypesRequest.getDefaultInstance());
    List<DataType> existingDataTypes = getResponse.getDataTypesList();
    assertEquals(1, existingDataTypes.size());
    assertEquals(dataTypeRule1, existingDataTypes.get(0).getRule());
  }

  @Test
  void createDataSetTest() {
    DataSetInfo dataSetInfo = createDataSetInfoForTest("data-set-1", true, List.of("1", "2"));
    CreateDataSetRequest request = CreateDataSetRequest.newBuilder().setInfo(dataSetInfo).build();
    CreateDataSetResponse response =
        dataClassificationConfigServiceBlockingStub.createDataSet(request);
    assertEquals(dataSetInfo, response.getDataSet().getInfo());
  }

  @Test
  void getDataSetNotExisting() {
    GetDataSetRequest request = GetDataSetRequest.newBuilder().setId("random-id").build();
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              dataClassificationConfigServiceBlockingStub.getDataSet(request);
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void getDataSetTest() {
    DataSetInfo dataSetInfo = createDataSetInfoForTest("data-set-1", true, List.of("1", "2"));
    CreateDataSetResponse response =
        dataClassificationConfigServiceBlockingStub.createDataSet(
            CreateDataSetRequest.newBuilder().setInfo(dataSetInfo).build());
    String id = response.getDataSet().getId();
    GetDataSetRequest getRequest = GetDataSetRequest.newBuilder().setId(id).build();
    GetDataSetResponse getResponse =
        dataClassificationConfigServiceBlockingStub.getDataSet(getRequest);
    assertEquals(dataSetInfo, getResponse.getDataSet().getInfo());
  }

  @Test
  void getDataSetsTest() {
    DataSetInfo dataSetInfo1 = createDataSetInfoForTest("data-set-1", true, List.of("1", "2"));
    DataSetInfo dataSetInfo2 = createDataSetInfoForTest("data-set-2", true, List.of("1", "3"));
    dataClassificationConfigServiceBlockingStub.createDataSet(
        CreateDataSetRequest.newBuilder().setInfo(dataSetInfo1).build());
    dataClassificationConfigServiceBlockingStub.createDataSet(
        CreateDataSetRequest.newBuilder().setInfo(dataSetInfo2).build());
    GetDataSetsResponse response =
        dataClassificationConfigServiceBlockingStub.getDataSets(
            GetDataSetsRequest.getDefaultInstance());
    assertEquals(2, response.getDataSetsCount());
    assertEquals(
        List.of(dataSetInfo2, dataSetInfo1),
        response.getDataSetsList().stream()
            .map(DataSet::getInfo)
            .collect(Collectors.toUnmodifiableList()));
  }

  @Test
  void updateDataSetNotExisting() {
    DataSetInfo dataSetInfo = createDataSetInfoForTest("data-set-1", true, List.of("1", "2"));
    UpdateDataSetRequest request =
        UpdateDataSetRequest.newBuilder().setId("random-id").setInfo(dataSetInfo).build();
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              dataClassificationConfigServiceBlockingStub.updateDataSet(request);
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void updateDataSetTest() {
    DataSetInfo dataSetInfo1 = createDataSetInfoForTest("data-set-1", true, List.of("1", "2"));
    DataSetInfo dataSetInfo2 = createDataSetInfoForTest("data-set-2", true, List.of("1", "3"));
    CreateDataSetResponse createResponse =
        dataClassificationConfigServiceBlockingStub.createDataSet(
            CreateDataSetRequest.newBuilder().setInfo(dataSetInfo1).build());
    String updateId = createResponse.getDataSet().getId();
    UpdateDataSetResponse updateResponse =
        dataClassificationConfigServiceBlockingStub.updateDataSet(
            UpdateDataSetRequest.newBuilder().setId(updateId).setInfo(dataSetInfo2).build());
    assertEquals(dataSetInfo2, updateResponse.getDataSet().getInfo());
  }

  @Test
  void deleteDataSetNotExisting() {
    DeleteDataSetRequest request = DeleteDataSetRequest.newBuilder().setId("random-id").build();
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              dataClassificationConfigServiceBlockingStub.deleteDataSet(request);
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void deleteDataSetTest() {
    DataSetInfo dataSetInfo = createDataSetInfoForTest("data-set-1", true, List.of("1", "2"));
    CreateDataSetResponse response =
        dataClassificationConfigServiceBlockingStub.createDataSet(
            CreateDataSetRequest.newBuilder().setInfo(dataSetInfo).build());
    String deleteId = response.getDataSet().getId();
    dataClassificationConfigServiceBlockingStub.deleteDataSet(
        DeleteDataSetRequest.newBuilder().setId(deleteId).build());
    GetDataSetsResponse getResponse =
        dataClassificationConfigServiceBlockingStub.getDataSets(
            GetDataSetsRequest.getDefaultInstance());
    assertEquals(0, getResponse.getDataSetsCount());
  }

  private DataSetInfo createDataSetInfoForTest(
      String name, boolean enabled, List<String> dataTypeIds) {
    return DataSetInfo.newBuilder()
        .setName(name)
        .setEnabled(enabled)
        .addAllDataTypeIds(dataTypeIds)
        .build();
  }

  private DataTypeRule createDataTypeRuleForTest(String name, String value) {
    return DataTypeRule.newBuilder()
        .setName(name)
        .addScopedPattern(
            ScopedPattern.newBuilder()
                .setApiScope(ApiScope.newBuilder().addAllApiIds(List.of("1", "2")))
                .setParameterTypeValue(1)
                .setKeyPattern(
                    StringPattern.newBuilder()
                        .setOperator(Operator.OPERATOR_EQUALS)
                        .setValue("value-1"))
                .setActionValue(1))
        .build();
  }
}
