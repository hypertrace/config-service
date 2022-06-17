package ai.traceable.data.classification.config.service;

import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_RAW_DATA_SET_DESCRIPTION;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_RAW_DATA_SET_ID;
import static ai.traceable.data.classification.config.service.RedactionRulesDao.LEGACY_RAW_DATA_SET_NAME;
import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_REQUEST_HEADER;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_REDACT;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.data.classification.config.service.v1.CreateDataSetRequest;
import ai.traceable.data.classification.config.service.v1.CreateDataSetResponse;
import ai.traceable.data.classification.config.service.v1.CreateDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.CreateDataTypeResponse;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
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
  Config mockConfig;
  private static final String DATA_CLASSIFICATION_CONFIG_SERVICE =
      "data.classification.config.service";
  boolean sensitiveDataConfigServiceMockFlag;

  @BeforeEach
  void setUp() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockConfig = mock(Config.class);
    when(mockConfig.hasPath(DATA_CLASSIFICATION_CONFIG_SERVICE)).thenReturn(false);
    mockGenericConfigService
        .addService(
            new DataClassificationConfigServiceImpl(
                new DataSetStore(genericStub, configChangeEventGenerator),
                new DataTypeStore(genericStub, configChangeEventGenerator),
                new DeletedDataSetStore(genericStub),
                new DataSetConfigRequestValidator(),
                new DataTypeConfigRequestValidator(),
                mockConfig,
                null,
                new RedactionRulesDao(
                    sensitiveDataConfigServiceBlockingStub,
                    new LegacyDataSetStore(genericStub, configChangeEventGenerator)),
                mock(FeatureCachingClient.class)))
        .addService(new MockSensitiveDataConfigService())
        .start();
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
    mockGenericConfigService.shutdown();
    sensitiveDataConfigServiceMockFlag = true;
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockConfig = mock(Config.class);
    when(mockConfig.hasPath(DATA_CLASSIFICATION_CONFIG_SERVICE)).thenReturn(false);
    mockGenericConfigService
        .addService(
            new DataClassificationConfigServiceImpl(
                new DataSetStore(genericStub, configChangeEventGenerator),
                new DataTypeStore(genericStub, configChangeEventGenerator),
                new DeletedDataSetStore(genericStub),
                new DataSetConfigRequestValidator(),
                new DataTypeConfigRequestValidator(),
                mockConfig,
                null,
                new RedactionRulesDao(
                    sensitiveDataConfigServiceBlockingStub,
                    new LegacyDataSetStore(genericStub, configChangeEventGenerator)),
                mock(FeatureCachingClient.class)))
        .addService(new MockSensitiveDataConfigService())
        .start();
    dataClassificationConfigServiceBlockingStub =
        DataClassificationConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());

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
    mockGenericConfigService.shutdown();
    sensitiveDataConfigServiceMockFlag = true;
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockConfig = mock(Config.class);
    when(mockConfig.hasPath(DATA_CLASSIFICATION_CONFIG_SERVICE)).thenReturn(false);
    mockGenericConfigService
        .addService(
            new DataClassificationConfigServiceImpl(
                new DataSetStore(genericStub, configChangeEventGenerator),
                new DataTypeStore(genericStub, configChangeEventGenerator),
                new DeletedDataSetStore(genericStub),
                new DataSetConfigRequestValidator(),
                new DataTypeConfigRequestValidator(),
                mockConfig,
                null,
                new RedactionRulesDao(
                    sensitiveDataConfigServiceBlockingStub,
                    new LegacyDataSetStore(genericStub, configChangeEventGenerator)),
                mock(FeatureCachingClient.class)))
        .addService(new MockSensitiveDataConfigService())
        .start();
    dataClassificationConfigServiceBlockingStub =
        DataClassificationConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());

    GetDataSetsRequest request = GetDataSetsRequest.getDefaultInstance();
    GetDataSetsResponse response = dataClassificationConfigServiceBlockingStub.getDataSets(request);
    assertEquals(2, response.getDataSetsCount());
    assertEquals("id-2", response.getDataSets(0).getInfo().getDataTypeIds(0));
    assertEquals("id-3", response.getDataSets(1).getInfo().getDataTypeIds(0));
  }

  @Test
  void getDataSetFromRedactionRulesTest() {
    mockGenericConfigService.shutdown();
    sensitiveDataConfigServiceMockFlag = true;
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockConfig = mock(Config.class);
    when(mockConfig.hasPath(DATA_CLASSIFICATION_CONFIG_SERVICE)).thenReturn(false);
    mockGenericConfigService
        .addService(
            new DataClassificationConfigServiceImpl(
                new DataSetStore(genericStub, configChangeEventGenerator),
                new DataTypeStore(genericStub, configChangeEventGenerator),
                new DeletedDataSetStore(genericStub),
                new DataSetConfigRequestValidator(),
                new DataTypeConfigRequestValidator(),
                mockConfig,
                null,
                new RedactionRulesDao(
                    sensitiveDataConfigServiceBlockingStub,
                    new LegacyDataSetStore(genericStub, configChangeEventGenerator)),
                mock(FeatureCachingClient.class)))
        .addService(new MockSensitiveDataConfigService())
        .start();
    dataClassificationConfigServiceBlockingStub =
        DataClassificationConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());

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
  void systemDataTypesTest() {
    mockGenericConfigService.shutdown();
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockConfig = mock(Config.class);
    when(mockConfig.hasPath(DATA_CLASSIFICATION_CONFIG_SERVICE)).thenReturn(true);
    String jsonString =
        "system : {\n"
            + "datatypes : {\n"
            + "rp1 : [\n"
            + "{\n"
            + "id : systemdatatyperp1,\n"
            + "rule : {\n"
            + "name : systemdatatyperulerp1,\n"
            + "scoped_patterns : [\n"
            + "{\n"
            + "global_scope : {},\n"
            + "locations : [LOCATION_REQUEST_HEADER],\n"
            + "key_pattern : {operator : OPERATOR_MATCHES_REGEX, value : systemvalue},\n"
            + "action : ACTION_MATCH\n"
            + "}\n"
            + "]\n"
            + "}\n"
            + "}\n"
            + "],\n"
            + "rp2 : [\n"
            + "{\n"
            + "id : systemdatatyperp2,\n"
            + "rule : {\n"
            + "name : systemdatatyperulerp2,\n"
            + "scoped_patterns : [\n"
            + "{\n"
            + "global_scope : {},\n"
            + "locations : [LOCATION_REQUEST_HEADER],\n"
            + "key_pattern : {operator : OPERATOR_MATCHES_REGEX, value : systemvalue},\n"
            + "action : ACTION_MATCH\n"
            + "}\n"
            + "]\n"
            + "}\n"
            + "}\n"
            + "]\n"
            + "}\n"
            + "}";
    Config dataClassificationConfig = ConfigFactory.parseString(jsonString);
    when(mockConfig.getConfig(DATA_CLASSIFICATION_CONFIG_SERVICE))
        .thenReturn(dataClassificationConfig);
    FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);
    when(featureCachingClient.isDataClassificationRp2Enabled(any()))
        .thenReturn(false)
        .thenReturn(true);
    mockGenericConfigService
        .addService(
            new DataClassificationConfigServiceImpl(
                new DataSetStore(genericStub, configChangeEventGenerator),
                new DataTypeStore(genericStub, configChangeEventGenerator),
                new DeletedDataSetStore(genericStub),
                new DataSetConfigRequestValidator(),
                new DataTypeConfigRequestValidator(),
                mockConfig,
                null,
                new RedactionRulesDao(
                    sensitiveDataConfigServiceBlockingStub,
                    new LegacyDataSetStore(genericStub, configChangeEventGenerator)),
                featureCachingClient))
        .addService(new MockSensitiveDataConfigService())
        .start();
    dataClassificationConfigServiceBlockingStub =
        DataClassificationConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());
    GetDataTypesRequest getRequest = GetDataTypesRequest.getDefaultInstance();

    // RP1 case
    GetDataTypesResponse response =
        dataClassificationConfigServiceBlockingStub.getDataTypes(getRequest);
    assertEquals(1, response.getDataTypesCount());
    DataType actualDataType = response.getDataTypes(0);
    assertEquals("systemdatatyperp1", actualDataType.getId());
    assertEquals("systemdatatyperulerp1", actualDataType.getRule().getName());
    assertEquals(
        LOCATION_REQUEST_HEADER,
        response
            .getDataTypesList()
            .get(0)
            .getRule()
            .getScopedPatternsList()
            .get(0)
            .getLocations(0));

    // RP2 case
    response = dataClassificationConfigServiceBlockingStub.getDataTypes(getRequest);
    assertEquals(1, response.getDataTypesCount());
    actualDataType = response.getDataTypes(0);
    assertEquals("systemdatatyperp2", actualDataType.getId());
    assertEquals("systemdatatyperulerp2", actualDataType.getRule().getName());
    assertEquals(
        LOCATION_REQUEST_HEADER,
        response
            .getDataTypesList()
            .get(0)
            .getRule()
            .getScopedPatternsList()
            .get(0)
            .getLocations(0));
  }

  @Test
  void systemDataSetsTest() {
    mockGenericConfigService.shutdown();
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockConfig = mock(Config.class);
    when(mockConfig.hasPath(DATA_CLASSIFICATION_CONFIG_SERVICE)).thenReturn(true);
    String jsonString =
        "{\n"
            + "  \"system\": {\n"
            + "    \"datasets\": {\n"
            + "    \"rp1\": [\n"
            + "      {\n"
            + "        \"id\": \"systemdatasetrp1\",\n"
            + "        \"info\": {\n"
            + "          \"name\": \"systemdatasetinforp1\",\n"
            + "          \"enabled\": \"true\",\n"
            + "          \"data_type_ids\": [\n"
            + "            \"datatyperp1-1\",\n"
            + "            \"datatyperp1-2\"\n"
            + "          ],\n"
            + "          \"data_suppression\": \"DATA_SUPPRESSION_RAW\"\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"rp2\": [\n"
            + "      {\n"
            + "        \"id\": \"systemdatasetrp2\",\n"
            + "        \"info\": {\n"
            + "          \"name\": \"systemdatasetinforp2\",\n"
            + "          \"enabled\": \"true\",\n"
            + "          \"data_type_ids\": [\n"
            + "            \"datatyperp2-1\",\n"
            + "            \"datatyperp2-2\"\n"
            + "          ],\n"
            + "          \"data_suppression\": \"DATA_SUPPRESSION_RAW\"\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "  }\n"
            + "}";
    Config dataClassificationConfig = ConfigFactory.parseString(jsonString);
    when(mockConfig.getConfig(DATA_CLASSIFICATION_CONFIG_SERVICE))
        .thenReturn(dataClassificationConfig);
    FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);
    when(featureCachingClient.isDataClassificationRp2Enabled(any()))
        .thenReturn(false)
        .thenReturn(false)
        .thenReturn(true)
        .thenReturn(true);
    mockGenericConfigService
        .addService(
            new DataClassificationConfigServiceImpl(
                new DataSetStore(genericStub, configChangeEventGenerator),
                new DataTypeStore(genericStub, configChangeEventGenerator),
                new DeletedDataSetStore(genericStub),
                new DataSetConfigRequestValidator(),
                new DataTypeConfigRequestValidator(),
                mockConfig,
                null,
                new RedactionRulesDao(
                    sensitiveDataConfigServiceBlockingStub,
                    new LegacyDataSetStore(genericStub, configChangeEventGenerator)),
                featureCachingClient))
        .addService(new MockSensitiveDataConfigService())
        .start();
    dataClassificationConfigServiceBlockingStub =
        DataClassificationConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());
    GetDataSetsRequest getRequest = GetDataSetsRequest.getDefaultInstance();

    // RP1 case
    GetDataSetsResponse response =
        dataClassificationConfigServiceBlockingStub.getDataSets(getRequest);
    assertEquals(1, response.getDataSetsCount());
    DataSet actualDataSet = response.getDataSets(0);
    assertEquals("systemdatasetrp1", actualDataSet.getId());
    assertEquals("systemdatasetinforp1", actualDataSet.getInfo().getName());
    assertEquals(
        List.of("datatyperp1-1", "datatyperp1-2"), actualDataSet.getInfo().getDataTypeIdsList());

    // RP2 case
    response = dataClassificationConfigServiceBlockingStub.getDataSets(getRequest);
    assertEquals(1, response.getDataSetsCount());
    actualDataSet = response.getDataSets(0);
    assertEquals("systemdatasetrp2", actualDataSet.getId());
    assertEquals("systemdatasetinforp2", actualDataSet.getInfo().getName());
    assertEquals(
        List.of("datatyperp2-1", "datatyperp2-2"), actualDataSet.getInfo().getDataTypeIdsList());
  }

  @Test
  void deleteSystemDataSetTest() {
    mockGenericConfigService.shutdown();
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockConfig = mock(Config.class);
    when(mockConfig.hasPath(DATA_CLASSIFICATION_CONFIG_SERVICE)).thenReturn(true);
    String jsonString =
        "{\n"
            + "  \"system\": {\n"
            + "  \"datasets\": {\n"
            + "    \"rp1\": [\n"
            + "      {\n"
            + "        \"id\": \"systemdataset\",\n"
            + "        \"info\": {\n"
            + "          \"name\": \"systemdatasetinfo\",\n"
            + "          \"enabled\": \"true\",\n"
            + "          \"data_type_ids\": [\n"
            + "            \"datatype-1\",\n"
            + "            \"datatype-2\"\n"
            + "          ],\n"
            + "          \"data_suppression\": \"DATA_SUPPRESSION_RAW\"\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "  }\n"
            + "}";
    Config dataClassificationConfig = ConfigFactory.parseString(jsonString);
    when(mockConfig.getConfig(DATA_CLASSIFICATION_CONFIG_SERVICE))
        .thenReturn(dataClassificationConfig);
    mockGenericConfigService
        .addService(
            new DataClassificationConfigServiceImpl(
                new DataSetStore(genericStub, configChangeEventGenerator),
                new DataTypeStore(genericStub, configChangeEventGenerator),
                new DeletedDataSetStore(genericStub),
                new DataSetConfigRequestValidator(),
                new DataTypeConfigRequestValidator(),
                mockConfig,
                null,
                new RedactionRulesDao(
                    sensitiveDataConfigServiceBlockingStub,
                    new LegacyDataSetStore(genericStub, configChangeEventGenerator)),
                mock(FeatureCachingClient.class)))
        .addService(new MockSensitiveDataConfigService())
        .start();
    dataClassificationConfigServiceBlockingStub =
        DataClassificationConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());
    GetDataSetsRequest getRequest = GetDataSetsRequest.getDefaultInstance();
    GetDataSetsResponse response =
        dataClassificationConfigServiceBlockingStub.getDataSets(getRequest);
    assertEquals(1, response.getDataSetsCount());
    DeleteDataSetRequest deleteRequest =
        DeleteDataSetRequest.newBuilder().setId("systemdataset").build();
    dataClassificationConfigServiceBlockingStub.deleteDataSet(deleteRequest);
    GetDataSetsResponse responseAfterDeletion =
        dataClassificationConfigServiceBlockingStub.getDataSets(getRequest);
    assertEquals(0, responseAfterDeletion.getDataSetsCount());
  }

  @Test
  void updateDeletedSystemDataSetTest() {
    mockGenericConfigService.shutdown();
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockConfig = mock(Config.class);
    when(mockConfig.hasPath(DATA_CLASSIFICATION_CONFIG_SERVICE)).thenReturn(true);
    String jsonString =
        "{\n"
            + "  \"system\": {\n"
            + "  \"datasets\": {\n"
            + "    \"rp1\": [\n"
            + "      {\n"
            + "        \"id\": \"systemdataset\",\n"
            + "        \"info\": {\n"
            + "          \"name\": \"systemdatasetinfo\",\n"
            + "          \"enabled\": \"true\",\n"
            + "          \"data_type_ids\": [\n"
            + "            \"datatype-1\",\n"
            + "            \"datatype-2\"\n"
            + "          ],\n"
            + "          \"data_suppression\": \"DATA_SUPPRESSION_RAW\"\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "  }\n"
            + "}";
    Config dataClassificationConfig = ConfigFactory.parseString(jsonString);
    when(mockConfig.getConfig(DATA_CLASSIFICATION_CONFIG_SERVICE))
        .thenReturn(dataClassificationConfig);
    mockGenericConfigService
        .addService(
            new DataClassificationConfigServiceImpl(
                new DataSetStore(genericStub, configChangeEventGenerator),
                new DataTypeStore(genericStub, configChangeEventGenerator),
                new DeletedDataSetStore(genericStub),
                new DataSetConfigRequestValidator(),
                new DataTypeConfigRequestValidator(),
                mockConfig,
                null,
                new RedactionRulesDao(
                    sensitiveDataConfigServiceBlockingStub,
                    new LegacyDataSetStore(genericStub, configChangeEventGenerator)),
                mock(FeatureCachingClient.class)))
        .start();
    dataClassificationConfigServiceBlockingStub =
        DataClassificationConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());
    UpdateDataSetRequest updateRequest =
        UpdateDataSetRequest.newBuilder()
            .setId("systemdataset")
            .setInfo(
                DataSetInfo.newBuilder()
                    .setName("name-1")
                    .setEnabled(true)
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW))
            .build();
    UpdateDataSetResponse updateResponse =
        dataClassificationConfigServiceBlockingStub.updateDataSet(updateRequest);
    assertEquals("name-1", updateResponse.getDataSet().getInfo().getName());
    DeleteDataSetRequest deleteRequest =
        DeleteDataSetRequest.newBuilder().setId("systemdataset").build();
    dataClassificationConfigServiceBlockingStub.deleteDataSet(deleteRequest);
    UpdateDataSetRequest updateRequest2 =
        UpdateDataSetRequest.newBuilder()
            .setId("systemdataset")
            .setInfo(
                DataSetInfo.newBuilder()
                    .setName("name-2")
                    .setEnabled(true)
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW))
            .build();
    Throwable exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> {
              dataClassificationConfigServiceBlockingStub.updateDataSet(updateRequest2);
            });
    assertEquals(Status.NOT_FOUND, Status.fromThrowable(exception));
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
  void updateSystemDataTypeTest() {
    mockGenericConfigService.shutdown();
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    ConfigServiceBlockingStub genericStub =
        ConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    mockConfig = mock(Config.class);
    when(mockConfig.hasPath(DATA_CLASSIFICATION_CONFIG_SERVICE)).thenReturn(true);
    String jsonString =
        "system : {\n"
            + "datatypes : {\n"
            + "rp1 : [\n"
            + "{\n"
            + "id : systemdatatyperp1,\n"
            + "rule : {\n"
            + "name : systemdatatyperulerp1,\n"
            + "scoped_patterns : [\n"
            + "{\n"
            + "global_scope : {},\n"
            + "locations : [LOCATION_REQUEST_HEADER],\n"
            + "key_pattern : {operator : OPERATOR_MATCHES_REGEX, value : systemvalue},\n"
            + "action : ACTION_MATCH\n"
            + "}\n"
            + "]\n"
            + "}\n"
            + "}\n"
            + "],\n"
            + "rp2 : [\n"
            + "{\n"
            + "id : systemdatatyperp2,\n"
            + "rule : {\n"
            + "name : systemdatatyperulerp2,\n"
            + "scoped_patterns : [\n"
            + "{\n"
            + "global_scope : {},\n"
            + "locations : [LOCATION_REQUEST_HEADER],\n"
            + "key_pattern : {operator : OPERATOR_MATCHES_REGEX, value : systemvalue},\n"
            + "action : ACTION_MATCH\n"
            + "}\n"
            + "]\n"
            + "}\n"
            + "}\n"
            + "]\n"
            + "}\n"
            + "}";
    Config dataClassificationConfig = ConfigFactory.parseString(jsonString);
    when(mockConfig.getConfig(DATA_CLASSIFICATION_CONFIG_SERVICE))
        .thenReturn(dataClassificationConfig);
    FeatureCachingClient featureCachingClient = mock(FeatureCachingClient.class);
    when(featureCachingClient.isDataClassificationRp2Enabled(any()))
        .thenReturn(false)
        .thenReturn(true);
    mockGenericConfigService
        .addService(
            new DataClassificationConfigServiceImpl(
                new DataSetStore(genericStub, configChangeEventGenerator),
                new DataTypeStore(genericStub, configChangeEventGenerator),
                new DeletedDataSetStore(genericStub),
                new DataSetConfigRequestValidator(),
                new DataTypeConfigRequestValidator(),
                mockConfig,
                null,
                new RedactionRulesDao(
                    sensitiveDataConfigServiceBlockingStub,
                    new LegacyDataSetStore(genericStub, configChangeEventGenerator)),
                featureCachingClient))
        .addService(new MockSensitiveDataConfigService())
        .start();
    dataClassificationConfigServiceBlockingStub =
        DataClassificationConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());

    // update rp1 system data type
    DataTypeRule updatedSystemDataTypeRule =
        createDataTypeRuleForTest("updatedsystemdatatyperp1", "value-rp1");
    UpdateDataTypeRequest request =
        UpdateDataTypeRequest.newBuilder()
            .setId("systemdatatyperp1")
            .setRule(updatedSystemDataTypeRule)
            .build();
    UpdateDataTypeResponse updateresponse =
        dataClassificationConfigServiceBlockingStub.updateDataType(request);
    assertEquals(updatedSystemDataTypeRule, updateresponse.getDataType().getRule());

    // update rp2 system data type
    updatedSystemDataTypeRule = createDataTypeRuleForTest("updatedsystemdatatyperp2", "value-rp2");
    request =
        UpdateDataTypeRequest.newBuilder()
            .setId("systemdatatyperp2")
            .setRule(updatedSystemDataTypeRule)
            .build();
    updateresponse = dataClassificationConfigServiceBlockingStub.updateDataType(request);
    assertEquals(updatedSystemDataTypeRule, updateresponse.getDataType().getRule());
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
    assertEquals(
        getDataSetInfoWithSensitivityForVerification(dataSetInfo), response.getDataSet().getInfo());
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
    assertEquals(
        getDataSetInfoWithSensitivityForVerification(dataSetInfo),
        getResponse.getDataSet().getInfo());
  }

  @Test
  void getDataSetsTest() {
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
    assertEquals(
        getDataSetInfoWithSensitivityForVerification(dataSetInfo2),
        updateResponse.getDataSet().getInfo());
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

  private DataSetInfo getDataSetInfoWithSensitivityForVerification(DataSetInfo dataSetInfo) {
    return dataSetInfo.toBuilder()
        .setSensitivity(DataSetInfo.Sensitivity.SENSITIVITY_MEDIUM)
        .build();
  }

  private DataTypeRule createDataTypeRuleForTest(String name, String value) {
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

  class MockSensitiveDataConfigService
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
              .build()
              .newBuilder()
              .setRedactionStrategy(REDACTION_STRATEGY_UNSPECIFIED)
              .build());
      responseObserver.onCompleted();
    }
  }
}
