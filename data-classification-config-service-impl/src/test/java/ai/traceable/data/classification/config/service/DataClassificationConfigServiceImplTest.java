package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_RAW_DATA_SET_DESCRIPTION;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_RAW_DATA_SET_ID;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_RAW_DATA_SET_NAME;
import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_REQUEST_HEADER;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_REDACT;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.data.classification.config.service.v1.CreateDataClassificationOverrideRequest;
import ai.traceable.data.classification.config.service.v1.CreateDataClassificationOverrideResponse;
import ai.traceable.data.classification.config.service.v1.CreateDataSetRequest;
import ai.traceable.data.classification.config.service.v1.CreateDataSetResponse;
import ai.traceable.data.classification.config.service.v1.CreateDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.CreateDataTypeResponse;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideFilter;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.Sensitivity;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ApiScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Operator;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.data.classification.config.service.v1.DeleteDataClassificationOverridesRequest;
import ai.traceable.data.classification.config.service.v1.DeleteDataClassificationOverridesResponse;
import ai.traceable.data.classification.config.service.v1.DeleteDataSetRequest;
import ai.traceable.data.classification.config.service.v1.DeleteDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.GetDataClassificationOverridesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataClassificationOverridesResponse;
import ai.traceable.data.classification.config.service.v1.GetDataSetRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetResponse;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetsResponse;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import ai.traceable.data.classification.config.service.v1.IdFilter;
import ai.traceable.data.classification.config.service.v1.ScopeFilter;
import ai.traceable.data.classification.config.service.v1.SystemDataSetVersion;
import ai.traceable.data.classification.config.service.v1.UpdateDataClassificationOverrideRequest;
import ai.traceable.data.classification.config.service.v1.UpdateDataClassificationOverrideResponse;
import ai.traceable.data.classification.config.service.v1.UpdateDataSetRequest;
import ai.traceable.data.classification.config.service.v1.UpdateDataSetResponse;
import ai.traceable.data.classification.config.service.v1.UpdateDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.UpdateDataTypeResponse;
import ai.traceable.sensitivedata.config.service.v1.ComplexData;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesResponse;
import ai.traceable.sensitivedata.config.service.v1.GetAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAutomaticSecretRedactionStrategyResponse;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeResponse;
import ai.traceable.sensitivedata.config.service.v1.MatchType;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import com.google.common.collect.Iterables;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DataClassificationConfigServiceImplTest {
  MockGenericConfigService mockGenericConfigService;
  DataClassificationConfigServiceBlockingStub dataClassificationConfigServiceBlockingStub;
  boolean sensitiveDataConfigServiceMockFlag;

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService()
            .mockUpsert()
            .mockGet()
            .mockGetAll()
            .mockDelete()
            .mockDeleteAll();
    mockGenericConfigService.addService(new MockSensitiveDataConfigService());
    dataClassificationConfigServiceBlockingStub =
        DataClassificationConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());
    sensitiveDataConfigServiceMockFlag = false;
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void redactionRulesToDataTypesTest() {
    sensitiveDataConfigServiceMockFlag = true;
    registerAndStartService();

    DataType expectedDataType1 =
        DataType.newBuilder()
            .setId("id-2")
            .setRule(DataTypeRule.newBuilder().setName("rule-2").setDescription("description-2"))
            .build();
    DataType expectedDataType2 =
        DataType.newBuilder()
            .setId("id-3")
            .setRule(DataTypeRule.newBuilder().setName("rule-3").setDescription("description-3"))
            .build();
    GetDataTypesRequest getRequest = GetDataTypesRequest.getDefaultInstance();
    GetDataTypesResponse response =
        dataClassificationConfigServiceBlockingStub.getDataTypes(getRequest);
    assertEquals(2, response.getDataTypesCount());
    assertEquals(expectedDataType1, response.getDataTypes(0));
    assertEquals(expectedDataType2, response.getDataTypes(1));
  }

  @Test
  void getDataSetsFromRedactionRulesTest() {
    sensitiveDataConfigServiceMockFlag = true;
    registerAndStartService();

    GetDataSetsRequest request = GetDataSetsRequest.getDefaultInstance();
    GetDataSetsResponse response = dataClassificationConfigServiceBlockingStub.getDataSets(request);
    assertEquals(2, response.getDataSetsCount());
    assertEquals("id-2", response.getDataSets(0).getInfo().getDataTypeIds(0));
    assertEquals("id-3", response.getDataSets(1).getInfo().getDataTypeIds(0));
  }

  @Test
  void getDataSetFromRedactionRulesTest() {
    sensitiveDataConfigServiceMockFlag = true;
    registerAndStartService();

    DataSetInfo expectedDataSetInfo =
        DataSetInfo.newBuilder()
            .setName(LEGACY_RAW_DATA_SET_NAME)
            .setDescription(LEGACY_RAW_DATA_SET_DESCRIPTION)
            .setEnabled(true)
            .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)
            .addDataTypeIds("id-3")
            .build();
    String dataSetId = LEGACY_RAW_DATA_SET_ID;
    GetDataSetRequest request = GetDataSetRequest.newBuilder().setId(dataSetId).build();
    GetDataSetResponse response = dataClassificationConfigServiceBlockingStub.getDataSet(request);
    assertEquals(expectedDataSetInfo, response.getDataSet().getInfo());
  }

  @Test
  void deleteSystemDataSetTest() {
    DataSet systemDataSet = DataSet.newBuilder().setId("systemdataset").build();
    DataClassificationConfig mockConfig = mock(DataClassificationConfig.class);
    when(mockConfig.isSystemDataSet(systemDataSet.getId())).thenReturn(true);
    when(mockConfig.getSystemDataSet(systemDataSet.getId())).thenReturn(Optional.of(systemDataSet));
    when(mockConfig.getSystemDataSets(SystemDataSetVersion.SYSTEM_DATA_SET_VERSION_UNSPECIFIED))
        .thenReturn(List.of(systemDataSet));
    registerAndStartService(mockConfig);
    GetDataSetsRequest getRequest = GetDataSetsRequest.getDefaultInstance();
    GetDataSetsResponse response =
        dataClassificationConfigServiceBlockingStub.getDataSets(getRequest);
    assertEquals(List.of(systemDataSet), response.getDataSetsList());
    DeleteDataSetRequest deleteRequest =
        DeleteDataSetRequest.newBuilder().setId(systemDataSet.getId()).build();
    dataClassificationConfigServiceBlockingStub.deleteDataSet(deleteRequest);
    GetDataSetsResponse responseAfterDeletion =
        dataClassificationConfigServiceBlockingStub.getDataSets(getRequest);
    assertEquals(0, responseAfterDeletion.getDataSetsCount());
  }

  @Test
  void updateDeletedSystemDataSetTest() {
    DataSet systemDataSet = DataSet.newBuilder().setId("systemdataset").build();
    DataClassificationConfig mockConfig = mock(DataClassificationConfig.class);
    when(mockConfig.getSystemDataSets(SystemDataSetVersion.SYSTEM_DATA_SET_VERSION_UNSPECIFIED))
        .thenReturn(List.of(systemDataSet));
    when(mockConfig.getSystemDataSet(systemDataSet.getId())).thenReturn(Optional.of(systemDataSet));
    when(mockConfig.isSystemDataSet(systemDataSet.getId())).thenReturn(true);
    registerAndStartService(mockConfig);
    UpdateDataSetRequest updateRequest =
        UpdateDataSetRequest.newBuilder()
            .setId(systemDataSet.getId())
            .setInfo(
                DataSetInfo.newBuilder()
                    .setName("name-1")
                    .setEnabled(true)
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW))
            .build();
    UpdateDataSetResponse updateResponse =
        dataClassificationConfigServiceBlockingStub.updateDataSet(updateRequest);
    assertEquals("name-1", updateResponse.getDataSet().getInfo().getName());
    assertEquals(
        List.of(updateResponse.getDataSet()),
        dataClassificationConfigServiceBlockingStub
            .getDataSets(GetDataSetsRequest.getDefaultInstance())
            .getDataSetsList());
    DeleteDataSetRequest deleteRequest =
        DeleteDataSetRequest.newBuilder().setId(systemDataSet.getId()).build();
    dataClassificationConfigServiceBlockingStub.deleteDataSet(deleteRequest);

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                dataClassificationConfigServiceBlockingStub.updateDataSet(
                    UpdateDataSetRequest.newBuilder()
                        .setId("systemdataset")
                        .setInfo(DataSetInfo.newBuilder().setName("name-2"))
                        .build()));
    assertEquals(Status.NOT_FOUND, exception.getStatus());
  }

  @Test
  void createDataTypeTest() {
    registerAndStartService();

    DataTypeRule dataTypeRule = createDataTypeRuleForTest("data-type-rule-1");
    CreateDataTypeRequest request =
        CreateDataTypeRequest.newBuilder().setRule(dataTypeRule).build();
    CreateDataTypeResponse response =
        dataClassificationConfigServiceBlockingStub.createDataType(request);
    assertEquals(dataTypeRule, response.getDataType().getRule());
  }

  @Test
  void getDataTypestest() {
    registerAndStartService();
    DataTypeRule dataTypeRule1 = createDataTypeRuleForTest("data-type-rule-1");
    DataTypeRule dataTypeRule2 = createDataTypeRuleForTest("data-type-rule-2");
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
    registerAndStartService();
    DataTypeRule dataTypeRule = createDataTypeRuleForTest("data-type-rule-1");
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
    registerAndStartService();
    DataTypeRule dataTypeRule1 = createDataTypeRuleForTest("data-type-rule-1");
    DataTypeRule dataTypeRule2 = createDataTypeRuleForTest("data-type-rule-2");
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
  void updateSystemDataTypeTest() {
    DataClassificationConfig mockConfig = mock(DataClassificationConfig.class);
    registerAndStartService(mockConfig);
    // update rp1 system data type
    DataTypeRule updatedSystemDataTypeRule = createDataTypeRuleForTest("updatedsystemdatatyperp1");
    UpdateDataTypeRequest request =
        UpdateDataTypeRequest.newBuilder()
            .setId("systemdatatyperp1")
            .setRule(updatedSystemDataTypeRule)
            .build();
    when(mockConfig.isSystemDataType("systemdatatyperp1")).thenReturn(true);
    UpdateDataTypeResponse updateresponse =
        dataClassificationConfigServiceBlockingStub.updateDataType(request);
    assertEquals(updatedSystemDataTypeRule, updateresponse.getDataType().getRule());

    // update rp2 system data type
    updatedSystemDataTypeRule = createDataTypeRuleForTest("updatedsystemdatatyperp2");
    request =
        UpdateDataTypeRequest.newBuilder()
            .setId("systemdatatyperp2")
            .setRule(updatedSystemDataTypeRule)
            .build();
    when(mockConfig.isSystemDataType("systemdatatyperp2")).thenReturn(true);
    updateresponse = dataClassificationConfigServiceBlockingStub.updateDataType(request);
    assertEquals(updatedSystemDataTypeRule, updateresponse.getDataType().getRule());
  }

  @Test
  void deleteDataTypeNotExisting() {
    registerAndStartService();
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
    registerAndStartService();
    DataTypeRule dataTypeRule1 = createDataTypeRuleForTest("data-type-rule-1");
    DataTypeRule dataTypeRule2 = createDataTypeRuleForTest("data-type-rule-2");
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
    registerAndStartService();
    DataSetInfo dataSetInfo = createDataSetInfoForTest("data-set-1", true, List.of("1", "2"));
    CreateDataSetRequest request = CreateDataSetRequest.newBuilder().setInfo(dataSetInfo).build();
    CreateDataSetResponse response =
        dataClassificationConfigServiceBlockingStub.createDataSet(request);
    assertEquals(
        getDataSetInfoWithSensitivityForVerification(dataSetInfo), response.getDataSet().getInfo());
  }

  @Test
  void getDataSetNotExisting() {
    registerAndStartService();
    GetDataSetRequest request = GetDataSetRequest.newBuilder().setId("random-id").build();
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> dataClassificationConfigServiceBlockingStub.getDataSet(request));
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void getDataSetTest() {
    registerAndStartService();
    DataSetInfo dataSetInfo = createDataSetInfoForTest("data-set-1", true, List.of("1", "2"));
    CreateDataSetResponse response =
        dataClassificationConfigServiceBlockingStub.createDataSet(
            CreateDataSetRequest.newBuilder().setInfo(dataSetInfo).build());
    String id = response.getDataSet().getId();
    GetDataSetRequest getRequest = GetDataSetRequest.newBuilder().setId(id).build();
    GetDataSetResponse getResponse =
        dataClassificationConfigServiceBlockingStub.getDataSet(getRequest);
    assertEquals(
        getDataSetInfoWithSensitivityForVerification(dataSetInfo),
        getResponse.getDataSet().getInfo());
  }

  @Test
  void getDataSetsTest() {
    registerAndStartService();
    DataSetInfo dataSetInfo1 = createDataSetInfoForTest("data-set-1", true, List.of("1", "2"));
    DataSetInfo dataSetInfo2 = createDataSetInfoForTest("data-set-2", true, List.of("1", "3"));
    String dataSetId1 =
        dataClassificationConfigServiceBlockingStub
            .createDataSet(CreateDataSetRequest.newBuilder().setInfo(dataSetInfo1).build())
            .getDataSet()
            .getId();
    dataClassificationConfigServiceBlockingStub.createDataSet(
        CreateDataSetRequest.newBuilder().setInfo(dataSetInfo2).build());
    GetDataSetsResponse response =
        dataClassificationConfigServiceBlockingStub.getDataSets(
            GetDataSetsRequest.getDefaultInstance());
    assertEquals(2, response.getDataSetsCount());
    assertEquals(
        List.of(
            getDataSetInfoWithSensitivityForVerification(dataSetInfo2),
            getDataSetInfoWithSensitivityForVerification(dataSetInfo1)),
        response.getDataSetsList().stream()
            .map(DataSet::getInfo)
            .collect(Collectors.toUnmodifiableList()));

    // ensure order is maintained post update
    DataSetInfo updateDataSetInfo1 = dataSetInfo1.toBuilder().setEnabled(false).build();
    dataClassificationConfigServiceBlockingStub.updateDataSet(
        UpdateDataSetRequest.newBuilder().setId(dataSetId1).setInfo(updateDataSetInfo1).build());
    response =
        dataClassificationConfigServiceBlockingStub.getDataSets(
            GetDataSetsRequest.getDefaultInstance());
    assertEquals(2, response.getDataSetsCount());
    assertEquals(
        List.of(
            getDataSetInfoWithSensitivityForVerification(dataSetInfo2),
            getDataSetInfoWithSensitivityForVerification(updateDataSetInfo1)),
        response.getDataSetsList().stream()
            .map(DataSet::getInfo)
            .collect(Collectors.toUnmodifiableList()));
  }

  @Test
  void updateDataSetNotExisting() {
    registerAndStartService();
    DataSetInfo dataSetInfo = createDataSetInfoForTest("data-set-1", true, List.of("1", "2"));
    UpdateDataSetRequest request =
        UpdateDataSetRequest.newBuilder().setId("random-id").setInfo(dataSetInfo).build();
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> dataClassificationConfigServiceBlockingStub.updateDataSet(request));
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void updateDataSetTest() {
    registerAndStartService();
    DataSetInfo dataSetInfo1 = createDataSetInfoForTest("data-set-1", true, List.of("1", "2"));
    DataSetInfo dataSetInfo2 = createDataSetInfoForTest("data-set-2", true, List.of("1", "3"));
    CreateDataSetResponse createResponse =
        dataClassificationConfigServiceBlockingStub.createDataSet(
            CreateDataSetRequest.newBuilder().setInfo(dataSetInfo1).build());
    String updateId = createResponse.getDataSet().getId();
    UpdateDataSetResponse updateResponse =
        dataClassificationConfigServiceBlockingStub.updateDataSet(
            UpdateDataSetRequest.newBuilder().setId(updateId).setInfo(dataSetInfo2).build());
    assertEquals(
        getDataSetInfoWithSensitivityForVerification(dataSetInfo2),
        updateResponse.getDataSet().getInfo());
  }

  @Test
  void deleteDataSetNotExisting() {
    registerAndStartService();
    DeleteDataSetRequest request = DeleteDataSetRequest.newBuilder().setId("random-id").build();
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> dataClassificationConfigServiceBlockingStub.deleteDataSet(request));
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
  }

  @Test
  void deleteDataSetTest() {
    registerAndStartService();
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

  @Test
  void createDataClassificationOverrideTest() {
    registerAndStartService();
    CreateDataClassificationOverrideRequest request =
        CreateDataClassificationOverrideRequest.newBuilder()
            .setDataClassificationOverrideRule(getDataClassificationOverrideRule())
            .build();
    assertEquals(
        request.getDataClassificationOverrideRule(),
        dataClassificationConfigServiceBlockingStub
            .createDataClassificationOverride(request)
            .getCreatedDataClassificationOverride()
            .getDataClassificationOverrideRule());
  }

  @Test
  void getDataClassificationOverridesByEnvironmentFilterTest() {
    registerAndStartService();
    DataClassificationOverrideRule rule1 = getDataClassificationOverrideRule();
    DataClassificationOverrideRule rule2 = getDataClassificationOverrideRule();
    CreateDataClassificationOverrideResponse dco1 =
        dataClassificationConfigServiceBlockingStub.createDataClassificationOverride(
            CreateDataClassificationOverrideRequest.newBuilder()
                .setDataClassificationOverrideRule(rule1)
                .build());
    CreateDataClassificationOverrideResponse dco2 =
        dataClassificationConfigServiceBlockingStub.createDataClassificationOverride(
            CreateDataClassificationOverrideRequest.newBuilder()
                .setDataClassificationOverrideRule(rule2)
                .build());
    GetDataClassificationOverridesResponse response =
        dataClassificationConfigServiceBlockingStub.getDataClassificationOverrides(
            GetDataClassificationOverridesRequest.newBuilder()
                .setFilter(
                    DataClassificationOverrideFilter.newBuilder()
                        .setScopeFilter(
                            ScopeFilter.newBuilder()
                                .addAllScopes(List.of(rule1.getScope(), rule2.getScope()))
                                .build())
                        .build())
                .build());
    assertEquals(2, response.getDataClassificationOverridesCount());
    assertEquals(
        dco2.getCreatedDataClassificationOverride(), response.getDataClassificationOverrides(0));
    assertEquals(
        dco1.getCreatedDataClassificationOverride(), response.getDataClassificationOverrides(1));
  }

  @Test
  void getDataClassificationOverridesByIdFilterTest() {
    registerAndStartService();
    DataClassificationOverrideRule rule1 = getDataClassificationOverrideRule();
    DataClassificationOverrideRule rule2 = getDataClassificationOverrideRule();
    CreateDataClassificationOverrideResponse dco1 =
        dataClassificationConfigServiceBlockingStub.createDataClassificationOverride(
            CreateDataClassificationOverrideRequest.newBuilder()
                .setDataClassificationOverrideRule(rule1)
                .build());
    CreateDataClassificationOverrideResponse dco2 =
        dataClassificationConfigServiceBlockingStub.createDataClassificationOverride(
            CreateDataClassificationOverrideRequest.newBuilder()
                .setDataClassificationOverrideRule(rule2)
                .build());
    GetDataClassificationOverridesResponse response =
        dataClassificationConfigServiceBlockingStub.getDataClassificationOverrides(
            GetDataClassificationOverridesRequest.newBuilder()
                .setFilter(
                    DataClassificationOverrideFilter.newBuilder()
                        .setIdFilter(
                            IdFilter.newBuilder()
                                .addAllIds(
                                    List.of(
                                        dco1.getCreatedDataClassificationOverride().getId(),
                                        dco2.getCreatedDataClassificationOverride().getId()))
                                .build())
                        .build())
                .build());
    assertEquals(2, response.getDataClassificationOverridesCount());
    assertEquals(
        dco2.getCreatedDataClassificationOverride(), response.getDataClassificationOverrides(0));
    assertEquals(
        dco1.getCreatedDataClassificationOverride(), response.getDataClassificationOverrides(1));
  }

  // No filter set gets all the data classification Overrides.
  @Test
  void getAllDataClassificationOverrides() {
    registerAndStartService();
    DataClassificationOverrideRule rule1 = getDataClassificationOverrideRule();
    DataClassificationOverrideRule rule2 = getDataClassificationOverrideRule();
    CreateDataClassificationOverrideResponse dco1 =
        dataClassificationConfigServiceBlockingStub.createDataClassificationOverride(
            CreateDataClassificationOverrideRequest.newBuilder()
                .setDataClassificationOverrideRule(rule1)
                .build());
    CreateDataClassificationOverrideResponse dco2 =
        dataClassificationConfigServiceBlockingStub.createDataClassificationOverride(
            CreateDataClassificationOverrideRequest.newBuilder()
                .setDataClassificationOverrideRule(rule2)
                .build());
    GetDataClassificationOverridesResponse response =
        dataClassificationConfigServiceBlockingStub.getDataClassificationOverrides(
            GetDataClassificationOverridesRequest.newBuilder().build());
    assertEquals(2, response.getDataClassificationOverridesCount());
    assertEquals(
        dco2.getCreatedDataClassificationOverride(), response.getDataClassificationOverrides(0));
    assertEquals(
        dco1.getCreatedDataClassificationOverride(), response.getDataClassificationOverrides(1));
  }

  @Test
  void getDataClassificationOverridesFilterRequestValidatorTest() {
    registerAndStartService();
    RequestContext requestContext = RequestContext.forTenantId("test_id");
    GetDataClassificationOverridesRequest request =
        GetDataClassificationOverridesRequest.newBuilder()
            .setFilter(DataClassificationOverrideFilter.newBuilder().build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            new DataClassificationOverrideConfigRequestValidator()
                .validateOrThrow(requestContext, request));
  }

  @Test
  void updateDataClassificationOverrideTest() {
    registerAndStartService();
    DataClassificationOverrideRule rule1 = getDataClassificationOverrideRule();
    DataClassificationOverrideRule rule2 = getDataClassificationOverrideRule();
    CreateDataClassificationOverrideResponse dco1 =
        dataClassificationConfigServiceBlockingStub.createDataClassificationOverride(
            CreateDataClassificationOverrideRequest.newBuilder()
                .setDataClassificationOverrideRule(rule1)
                .build());
    UpdateDataClassificationOverrideResponse response =
        dataClassificationConfigServiceBlockingStub.updateDataClassificationOverride(
            UpdateDataClassificationOverrideRequest.newBuilder()
                .setId(dco1.getCreatedDataClassificationOverride().getId())
                .setDataClassificationOverrideRule(rule2)
                .build());
    assertEquals(
        dco1.getCreatedDataClassificationOverride().getId(),
        response.getUpdatedDataClassificationOverride().getId());
    assertEquals(
        rule2, response.getUpdatedDataClassificationOverride().getDataClassificationOverrideRule());
  }

  @Test
  void deleteDataClassificationOverridesByEnvironmentFilterTest() {
    registerAndStartService();
    DataClassificationOverrideRule rule1 = getDataClassificationOverrideRule();
    DataClassificationOverrideRule rule2 = getDataClassificationOverrideRule();
    CreateDataClassificationOverrideResponse dco1 =
        dataClassificationConfigServiceBlockingStub.createDataClassificationOverride(
            CreateDataClassificationOverrideRequest.newBuilder()
                .setDataClassificationOverrideRule(rule1)
                .build());
    CreateDataClassificationOverrideResponse dco2 =
        dataClassificationConfigServiceBlockingStub.createDataClassificationOverride(
            CreateDataClassificationOverrideRequest.newBuilder()
                .setDataClassificationOverrideRule(rule2)
                .build());
    DeleteDataClassificationOverridesResponse response =
        dataClassificationConfigServiceBlockingStub.deleteDataClassificationOverrides(
            DeleteDataClassificationOverridesRequest.newBuilder()
                .setFilter(
                    DataClassificationOverrideFilter.newBuilder()
                        .setScopeFilter(
                            ScopeFilter.newBuilder()
                                .addAllScopes(List.of(rule1.getScope(), rule2.getScope()))
                                .build())
                        .build())
                .build());
    assertEquals(2, response.getDeletedDataClassificationOverridesCount());
    assertEquals(
        dco2.getCreatedDataClassificationOverride(),
        response.getDeletedDataClassificationOverrides(0));
    assertEquals(
        dco1.getCreatedDataClassificationOverride(),
        response.getDeletedDataClassificationOverrides(1));
  }

  @Test
  void deleteDataClassificationOverrideByIdFilterTest() {
    registerAndStartService();
    DataClassificationOverrideRule rule1 = getDataClassificationOverrideRule();
    DataClassificationOverrideRule rule2 = getDataClassificationOverrideRule();
    CreateDataClassificationOverrideResponse dco1 =
        dataClassificationConfigServiceBlockingStub.createDataClassificationOverride(
            CreateDataClassificationOverrideRequest.newBuilder()
                .setDataClassificationOverrideRule(rule1)
                .build());
    CreateDataClassificationOverrideResponse dco2 =
        dataClassificationConfigServiceBlockingStub.createDataClassificationOverride(
            CreateDataClassificationOverrideRequest.newBuilder()
                .setDataClassificationOverrideRule(rule2)
                .build());
    DeleteDataClassificationOverridesResponse response =
        dataClassificationConfigServiceBlockingStub.deleteDataClassificationOverrides(
            DeleteDataClassificationOverridesRequest.newBuilder()
                .setFilter(
                    DataClassificationOverrideFilter.newBuilder()
                        .setIdFilter(
                            IdFilter.newBuilder()
                                .addAllIds(
                                    List.of(
                                        dco1.getCreatedDataClassificationOverride().getId(),
                                        dco2.getCreatedDataClassificationOverride().getId()))
                                .build())
                        .build())
                .build());
    assertEquals(2, response.getDeletedDataClassificationOverridesCount());
    assertEquals(
        dco2.getCreatedDataClassificationOverride(),
        response.getDeletedDataClassificationOverrides(0));
    assertEquals(
        dco1.getCreatedDataClassificationOverride(),
        response.getDeletedDataClassificationOverrides(1));
  }

  // No filter set deletes all the data classification Overrides.
  @Test
  void deleteAllDataClassificationOverrides() {
    registerAndStartService();
    DataClassificationOverrideRule rule1 = getDataClassificationOverrideRule();
    DataClassificationOverrideRule rule2 = getDataClassificationOverrideRule();
    CreateDataClassificationOverrideResponse dco1 =
        dataClassificationConfigServiceBlockingStub.createDataClassificationOverride(
            CreateDataClassificationOverrideRequest.newBuilder()
                .setDataClassificationOverrideRule(rule1)
                .build());
    CreateDataClassificationOverrideResponse dco2 =
        dataClassificationConfigServiceBlockingStub.createDataClassificationOverride(
            CreateDataClassificationOverrideRequest.newBuilder()
                .setDataClassificationOverrideRule(rule2)
                .build());
    DeleteDataClassificationOverridesResponse response =
        dataClassificationConfigServiceBlockingStub.deleteDataClassificationOverrides(
            DeleteDataClassificationOverridesRequest.newBuilder().build());
    assertEquals(2, response.getDeletedDataClassificationOverridesCount());
    assertEquals(
        dco2.getCreatedDataClassificationOverride(),
        response.getDeletedDataClassificationOverrides(0));
    assertEquals(
        dco1.getCreatedDataClassificationOverride(),
        response.getDeletedDataClassificationOverrides(1));
  }

  @Test
  void deleteDataClassificationOverridesFilterRequestValidatorTest() {
    registerAndStartService();
    RequestContext requestContext = RequestContext.forTenantId("test_id");
    DeleteDataClassificationOverridesRequest request =
        DeleteDataClassificationOverridesRequest.newBuilder()
            .setFilter(DataClassificationOverrideFilter.newBuilder().build())
            .build();
    assertThrows(
        StatusRuntimeException.class,
        () ->
            new DataClassificationOverrideConfigRequestValidator()
                .validateOrThrow(requestContext, request));
  }

  @Test
  void getRp1DataTypesIfRequested() {
    List<DataType> rp1DataTypes = List.of(DataType.newBuilder().setId("rp1-dataset").build());
    DataClassificationConfig mockConfig = mock(DataClassificationConfig.class);
    when(mockConfig.getSystemDataTypes(SystemDataSetVersion.SYSTEM_DATA_SET_VERSION_RP1))
        .thenReturn(rp1DataTypes);
    registerAndStartService(mockConfig);
    GetDataTypesResponse response =
        dataClassificationConfigServiceBlockingStub.getDataTypes(
            GetDataTypesRequest.newBuilder()
                .setSystemDataSetVersion(SystemDataSetVersion.SYSTEM_DATA_SET_VERSION_RP1)
                .build());
    assertEquals(rp1DataTypes, response.getDataTypesList());
  }

  @Test
  void testDataTypeResolution() {
    registerAndStartService();
    DataType createdDataType =
        dataClassificationConfigServiceBlockingStub
            .createDataType(
                CreateDataTypeRequest.newBuilder()
                    .setRule(createDataTypeRuleForTest("data-type-rule-1"))
                    .build())
            .getDataType();
    dataClassificationConfigServiceBlockingStub.createDataSet(
        CreateDataSetRequest.newBuilder()
            .setInfo(
                DataSetInfo.newBuilder()
                    .setName("data-set-1")
                    .setEnabled(false)
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_OBFUSCATE)
                    .setSensitivity(Sensitivity.SENSITIVITY_CRITICAL)
                    .setColor("blue")
                    .addDataTypeIds(createdDataType.getId()))
            .build());

    // Default fetch should match the created type
    assertEquals(
        createdDataType,
        Iterables.getOnlyElement(
            dataClassificationConfigServiceBlockingStub
                .getDataTypes(GetDataTypesRequest.getDefaultInstance())
                .getDataTypesList()));

    // Resolved fetch should merge data set fields in
    DataType expectedResolvedType =
        createdDataType.toBuilder()
            .setRule(
                createdDataType.getRule().toBuilder()
                    .setSensitivity(Sensitivity.SENSITIVITY_CRITICAL)
                    .setEnabled(false)
                    .setColor("blue")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_OBFUSCATE))
            .build();
    assertEquals(
        expectedResolvedType,
        Iterables.getOnlyElement(
            dataClassificationConfigServiceBlockingStub
                .getDataTypes(
                    GetDataTypesRequest.newBuilder().setResolveInheritedDetails(true).build())
                .getDataTypesList()));
  }

  private DataSetInfo createDataSetInfoForTest(
      String name, boolean enabled, List<String> dataTypeIds) {
    return DataSetInfo.newBuilder()
        .setName(name)
        .setEnabled(enabled)
        .addAllDataTypeIds(dataTypeIds)
        .build();
  }

  private DataSetInfo getDataSetInfoWithSensitivityForVerification(DataSetInfo dataSetInfo) {
    return dataSetInfo.toBuilder()
        .setSensitivity(DataSetInfo.Sensitivity.SENSITIVITY_MEDIUM)
        .build();
  }

  private DataTypeRule createDataTypeRuleForTest(String name) {
    return DataTypeRule.newBuilder()
        .setName(name)
        .addScopedPatterns(
            ScopedPattern.newBuilder()
                .setApiScope(ApiScope.newBuilder().addAllApiIds(List.of("1", "2")))
                .addLocations(LOCATION_REQUEST_HEADER)
                .setKeyPattern(
                    StringPattern.newBuilder()
                        .setOperator(Operator.OPERATOR_EQUALS)
                        .setValue("value-1"))
                .setActionValue(1))
        .build();
  }

  private DataClassificationOverrideRule getDataClassificationOverrideRule() {
    return DataClassificationOverrideRule.newBuilder()
        .setScope(
            DataClassificationOverrideRule.DataClassificationOverrideScope.newBuilder()
                .setEnvironmentScope(
                    DataClassificationOverrideRule.EnvironmentScope.newBuilder()
                        .setEnvironmentId(
                            new StringBuilder("Environment_").append(UUID.randomUUID()).toString())
                        .build())
                .build())
        .setDataSuppressionOverride(
            DataClassificationOverrideRule.DataSuppressionOverride.newBuilder()
                .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)
                .build())
        .build();
  }

  private void registerAndStartService() {
    registerAndStartService(mock(DataClassificationConfig.class));
  }

  private void registerAndStartService(DataClassificationConfig config) {
    ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    DataClassificationConfigServiceBlockingStub ownStub =
        DataClassificationConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    DataTypeStore dataTypeStore = new DataTypeStore(genericStub, configChangeEventGenerator);
    RedactionRulesDao redactionRulesDao =
        new RedactionRulesDao(
            sensitiveDataConfigServiceBlockingStub,
            new LegacyDataSetStore(genericStub, configChangeEventGenerator));
    mockGenericConfigService
        .addService(
            new DataClassificationConfigServiceImpl(
                new DataSetStore(genericStub, configChangeEventGenerator),
                dataTypeStore,
                new DeletedDataSetStore(genericStub),
                new DataClassificationOverrideStore(genericStub, configChangeEventGenerator),
                new DataSetConfigRequestValidator(),
                new DataTypeConfigRequestValidator(),
                new DataClassificationOverrideConfigRequestValidator(),
                null,
                redactionRulesDao,
                new DataTypeResolver(ownStub),
                config))
        .start();
  }

  private class MockSensitiveDataConfigService
      extends SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceImplBase {

    @Override
    public void getAllRedactionRules(
        GetAllRedactionRulesRequest request,
        StreamObserver<GetAllRedactionRulesResponse> responseObserver) {
      GetAllRedactionRulesResponse.Builder responseBuilder =
          GetAllRedactionRulesResponse.newBuilder();
      if (sensitiveDataConfigServiceMockFlag) {
        RedactionRule rule1 =
            RedactionRule.newBuilder()
                .setId("id-1")
                .setName("rule-1")
                .setDescription("description-1")
                .setCategory("category-1")
                .setMatchType(MatchType.MATCH_TYPE_KEY)
                .setComplexData(ComplexData.getDefaultInstance())
                .setRegex("regex*")
                .setSessionIdentifier(true)
                .setFqn(false)
                .setRedactionStrategy(REDACTION_STRATEGY_REDACT)
                .build();
        RedactionRule rule2 =
            RedactionRule.newBuilder()
                .setId("id-2")
                .setName("rule-2")
                .setDescription("description-2")
                .setCategory("category-2")
                .setMatchType(MatchType.MATCH_TYPE_KEY)
                .setComplexData(ComplexData.getDefaultInstance())
                .setRegex("reg*ex")
                .setSessionIdentifier(false)
                .setFqn(true)
                .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_HASH)
                .build();
        RedactionRule rule3 =
            RedactionRule.newBuilder()
                .setId("id-3")
                .setName("rule-3")
                .setDescription("description-3")
                .setCategory("category-3")
                .setMatchType(MatchType.MATCH_TYPE_KEY)
                .setComplexData(ComplexData.getDefaultInstance())
                .setRegex("regex")
                .setSessionIdentifier(false)
                .setFqn(false)
                .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_RAW)
                .build();
        responseBuilder.addAllRedactionRules(List.of(rule1, rule2, rule3));
      }
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    }

    @Override
    public void getAutomaticSecretRedactionStrategy(
        GetAutomaticSecretRedactionStrategyRequest request,
        StreamObserver<GetAutomaticSecretRedactionStrategyResponse> responseObserver) {
      responseObserver.onNext(
          GetAutomaticSecretRedactionStrategyResponse.newBuilder().setEnabled(false).build());
      responseObserver.onCompleted();
    }

    @Override
    public void getRedactionStrategyForType(
        GetRedactionStrategyForTypeRequest request,
        StreamObserver<GetRedactionStrategyForTypeResponse> responseObserver) {
      responseObserver.onNext(
          GetRedactionStrategyForTypeResponse.newBuilder()
              .setRedactionStrategy(REDACTION_STRATEGY_UNSPECIFIED)
              .build());
      responseObserver.onCompleted();
    }
  }
}
