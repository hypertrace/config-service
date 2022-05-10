package ai.traceable.external.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.Sensitivity;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Action;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.EnvironmentScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.GlobalScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.KeyValuePattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Location;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Operator;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetsResponse;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import ai.traceable.external.data.classification.config.service.v1.ExternalDataClassificationServiceGrpc;
import ai.traceable.external.data.classification.config.service.v1.ExternalDataClassificationServiceGrpc.ExternalDataClassificationServiceBlockingStub;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigResponse;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesResponse;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import com.typesafe.config.Config;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ExternalDataClassificationConfigServiceImplTest {
  MockGenericConfigService mockGenericConfigService;
  Config mockConfig;
  Config mockExternalDataClassificationConfig;
  ExternalDataClassificationServiceBlockingStub externalDataClassificationServiceBlockingStub;
  SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub;
  DataClassificationConfigServiceBlockingStub dataClassificationConfigServiceBlockingStub;
  UuidGenerator uuidGenerator;
  private static final String EXTERNAL_DATA_CLASSIFICATION_CONFIG_SERVICE =
      "external.data.classification.config.service";
  private static final String DATA_PARSING_RULES = "data.parsing.rules";

  @BeforeEach
  void setup() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    mockConfig = mock(Config.class);
    mockExternalDataClassificationConfig = mock(Config.class);
    sensitiveDataConfigServiceBlockingStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    dataClassificationConfigServiceBlockingStub =
        DataClassificationConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());
    uuidGenerator = new UuidGenerator();
    when(mockConfig.getConfig(EXTERNAL_DATA_CLASSIFICATION_CONFIG_SERVICE))
        .thenReturn(mockExternalDataClassificationConfig);
    when(mockExternalDataClassificationConfig.getObjectList(DATA_PARSING_RULES)).thenReturn(null);
    mockGenericConfigService
        .addService(
            new ExternalDataClassificationConfigServiceImpl(
                mockConfig,
                new ExternalDataClassificationConfigRequestValidator(),
                new RedactionRulesDao(sensitiveDataConfigServiceBlockingStub),
                new DataClassificationRulesDao(dataClassificationConfigServiceBlockingStub),
                new RedactionRulesTranslator(),
                new DataClassificationRulesTranslator(),
                new ExternalDataClassificationRuleResponseBuilder(uuidGenerator)))
        .addService(new MockSensitiveDataConfigService())
        .addService(new MockDataClassificationConfigService())
        .start();
    externalDataClassificationServiceBlockingStub =
        ExternalDataClassificationServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());
  }

  @AfterEach
  void teardown() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void getDataClassificationConfigTest() {
    GetDataClassificationConfigResponse response =
        externalDataClassificationServiceBlockingStub.getDataClassificationConfig(
            GetDataClassificationConfigRequest.getDefaultInstance());
    assertEquals(2, response.getDataTypesCount());
  }

  class MockDataClassificationConfigService
      extends DataClassificationConfigServiceGrpc.DataClassificationConfigServiceImplBase {
    @Override
    public void getDataSets(
        GetDataSetsRequest request, StreamObserver<GetDataSetsResponse> responseObserver) {
      DataSet dataSet1 =
          DataSet.newBuilder()
              .setId("dataset-1")
              .setInfo(
                  DataSetInfo.newBuilder()
                      .setName("datasetname-1")
                      .setEnabled(true)
                      .setDataSuppression(DataSuppression.DATA_SUPPRESSION_OBFUSCATE)
                      .setSensitivity(Sensitivity.SENSITIVITY_MEDIUM)
                      .addDataTypeIds("datatype-1"))
              .build();
      DataSet dataSet2 =
          DataSet.newBuilder()
              .setId("dataset-2")
              .setInfo(
                  DataSetInfo.newBuilder()
                      .setName("datasetname-2")
                      .setEnabled(true)
                      .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
                      .setSensitivity(Sensitivity.SENSITIVITY_HIGH)
                      .addDataTypeIds("datatype-2"))
              .build();
      DataSet dataSet3 =
          DataSet.newBuilder()
              .setId("dataset-3")
              .setInfo(
                  DataSetInfo.newBuilder()
                      .setName("datatsetname-3")
                      .setEnabled(true)
                      .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)
                      .setSensitivity(Sensitivity.SENSITIVITY_LOW)
                      .addDataTypeIds("datatype-1")
                      .addDataTypeIds("datatype-2"))
              .build();
      DataSet dataSet4 =
          DataSet.newBuilder()
              .setId("dataset-4")
              .setInfo(
                  DataSetInfo.newBuilder()
                      .setName("datatsetname-4")
                      .setEnabled(false)
                      .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)
                      .setSensitivity(Sensitivity.SENSITIVITY_LOW)
                      .addDataTypeIds("datatype-3"))
              .build();
      responseObserver.onNext(
          GetDataSetsResponse.newBuilder()
              .addAllDataSets(List.of(dataSet1, dataSet2, dataSet3))
              .build());
      responseObserver.onCompleted();
    }

    @Override
    public void getDataTypes(
        GetDataTypesRequest request, StreamObserver<GetDataTypesResponse> responseObserver) {
      DataType dataType1 =
          DataType.newBuilder()
              .setId("datatype-1")
              .setRule(
                  DataTypeRule.newBuilder()
                      .setName("datatypename-1")
                      .addScopedPatterns(
                          ScopedPattern.newBuilder()
                              .setGlobalScope(GlobalScope.newBuilder())
                              .addLocations(Location.LOCATION_ANY)
                              .setKeyPattern(
                                  StringPattern.newBuilder()
                                      .setValue("val-1")
                                      .setOperator(Operator.OPERATOR_EQUALS))
                              .setAction(Action.ACTION_MATCH)))
              .build();

      DataType dataType2 =
          DataType.newBuilder()
              .setId("datatype-2")
              .setRule(
                  DataTypeRule.newBuilder()
                      .setName("datatypename-2")
                      .addScopedPatterns(
                          ScopedPattern.newBuilder()
                              .setGlobalScope(GlobalScope.newBuilder().build())
                              .addLocations(Location.LOCATION_ANY)
                              .setKeyPattern(
                                  StringPattern.newBuilder()
                                      .setValue("val-2")
                                      .setOperator(Operator.OPERATOR_EQUALS))
                              .setAction(Action.ACTION_MATCH)))
              .build();

      DataType dataType3 =
          DataType.newBuilder()
              .setId("id-3")
              .setRule(
                  DataTypeRule.newBuilder()
                      .setName("datatype-3")
                      .addScopedPatterns(
                          ScopedPattern.newBuilder()
                              .setEnvironmentScope(
                                  EnvironmentScope.newBuilder().addEnvironmentIds("env-1"))
                              .addLocations(Location.LOCATION_REQUEST_HEADER)
                              .setKeyValuePattern(
                                  KeyValuePattern.newBuilder()
                                      .setKeyPattern(
                                          StringPattern.newBuilder()
                                              .setValue("key-3")
                                              .setOperator(Operator.OPERATOR_EQUALS))
                                      .setValuePattern(
                                          StringPattern.newBuilder()
                                              .setValue("value-3")
                                              .setOperator(Operator.OPERATOR_EQUALS)))
                              .setAction(Action.ACTION_MATCH)))
              .build();

      responseObserver.onNext(
          GetDataTypesResponse.newBuilder()
              .addAllDataTypes(List.of(dataType1, dataType2, dataType3))
              .build());
      responseObserver.onCompleted();
    }
  }

  class MockSensitiveDataConfigService
      extends SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceImplBase {

    @Override
    public void getAllRedactionRules(
        GetAllRedactionRulesRequest request,
        StreamObserver<GetAllRedactionRulesResponse> responseObserver) {
      GetAllRedactionRulesResponse.Builder responseBuilder =
          GetAllRedactionRulesResponse.newBuilder();
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    }
  }
}
