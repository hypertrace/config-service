package ai.traceable.external.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverride;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule;
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
import ai.traceable.data.classification.config.service.v1.GetDataClassificationOverridesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataClassificationOverridesResponse;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetsResponse;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import ai.traceable.external.data.classification.config.service.v1.DataType.DataTypeMatchRule;
import ai.traceable.external.data.classification.config.service.v1.DataType.Result;
import ai.traceable.external.data.classification.config.service.v1.ExternalDataClassificationServiceGrpc;
import ai.traceable.external.data.classification.config.service.v1.ExternalDataClassificationServiceGrpc.ExternalDataClassificationServiceBlockingStub;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.EnvironmentFilter;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigRequest.OnlyIfChangedFilter;
import ai.traceable.external.data.classification.config.service.v1.GetDataClassificationConfigResponse;
import ai.traceable.external.data.classification.config.service.v1.PathPredicate;
import ai.traceable.external.data.classification.config.service.v1.StringPredicate;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesResponse;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeResponse;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ExternalDataClassificationConfigServiceImplTest {
  MockGenericConfigService mockGenericConfigService;
  ExternalDataClassificationServiceBlockingStub externalDataClassificationServiceBlockingStub;
  SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceBlockingStub;
  DataClassificationConfigServiceBlockingStub dataClassificationConfigServiceBlockingStub;
  InsightsServiceCoordinator insightsServiceCoordinator;
  UuidGenerator uuidGenerator;
  FeatureCachingClient featureCachingClient;
  List<DataClassificationOverride> dataClassificationOverrides;

  ExternalDataClassificationConfig externalDataClassificationConfig;

  @BeforeEach
  void setup() {
    mockGenericConfigService =
        new MockGenericConfigService().mockUpsert().mockGet().mockGetAll().mockDelete();
    sensitiveDataConfigServiceBlockingStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(this.mockGenericConfigService.channel());
    dataClassificationConfigServiceBlockingStub =
        DataClassificationConfigServiceGrpc.newBlockingStub(
            this.mockGenericConfigService.channel());
    uuidGenerator = new UuidGenerator();
    featureCachingClient = mock(FeatureCachingClient.class);
    when(featureCachingClient.isDataClassificationRp2Enabled(any())).thenReturn(true);
    insightsServiceCoordinator = mock(InsightsServiceCoordinatorImpl.class);
    when(insightsServiceCoordinator.getSensitiveHeaderParameters(RequestContext.CURRENT.get()))
        .thenReturn(List.of());
    externalDataClassificationConfig = mock(ExternalDataClassificationConfig.class);
    mockGenericConfigService
        .addService(
            new ExternalDataClassificationConfigServiceImpl(
                externalDataClassificationConfig,
                new ExternalDataClassificationConfigRequestValidator(),
                new RedactionRulesDao(sensitiveDataConfigServiceBlockingStub),
                new DataClassificationRulesDao(dataClassificationConfigServiceBlockingStub),
                new RedactionRulesTranslator(),
                new DataClassificationRulesTranslator(),
                new ExternalDataClassificationRuleResponseBuilder(uuidGenerator),
                insightsServiceCoordinator,
                featureCachingClient))
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
    dataClassificationOverrides = new ArrayList<>();
    GetDataClassificationConfigResponse response =
        externalDataClassificationServiceBlockingStub.getDataClassificationConfig(
            GetDataClassificationConfigRequest.getDefaultInstance());
    assertEquals(1, response.getDataTypesCount());

    response =
        externalDataClassificationServiceBlockingStub.getDataClassificationConfig(
            GetDataClassificationConfigRequest.newBuilder()
                .setChangeFilter(OnlyIfChangedFilter.getDefaultInstance())
                .setEnvironmentFilter(EnvironmentFilter.newBuilder().setEnvironmentName("random"))
                .build());
    assertEquals(1, response.getDataTypesCount());

    response =
        externalDataClassificationServiceBlockingStub.getDataClassificationConfig(
            GetDataClassificationConfigRequest.newBuilder()
                .setChangeFilter(OnlyIfChangedFilter.getDefaultInstance())
                .setEnvironmentFilter(EnvironmentFilter.newBuilder().setEnvironmentName("env-1"))
                .build());
    assertEquals(3, response.getDataTypesCount());
  }

  @Test
  void getDataClassificationConfigTestWithDataClassificationOverrides() {
    dataClassificationOverrides =
        List.of(
            DataClassificationOverride.newBuilder()
                .setDataClassificationOverrideRule(
                    DataClassificationOverrideRule.newBuilder()
                        .setDataSuppressionOverride(
                            DataClassificationOverrideRule.DataSuppressionOverride.newBuilder()
                                .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)))
                .build());
    GetDataClassificationConfigResponse response =
        externalDataClassificationServiceBlockingStub.getDataClassificationConfig(
            GetDataClassificationConfigRequest.newBuilder()
                .setChangeFilter(OnlyIfChangedFilter.getDefaultInstance())
                .setEnvironmentFilter(EnvironmentFilter.newBuilder().setEnvironmentName("random"))
                .build());
    assertEquals(0, response.getDataTypesCount());
  }

  @Test
  void returnsEmptyDisabledResponseIfDisabled() {
    dataClassificationOverrides = new ArrayList<>();
    when(featureCachingClient.isDataClassificationRp2Enabled(any())).thenReturn(false);
    assertEquals(
        GetDataClassificationConfigResponse.newBuilder().setEnabled(false).build(),
        externalDataClassificationServiceBlockingStub.getDataClassificationConfig(
            GetDataClassificationConfigRequest.getDefaultInstance()));
  }

  @Test
  void returnsDefaultRule() {
    dataClassificationOverrides = new ArrayList<>();
    ai.traceable.external.data.classification.config.service.v1.DataType defaultDataType =
        ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
            .addMatchRules(
                DataTypeMatchRule.newBuilder()
                    .setResult(Result.RESULT_MATCH)
                    .setPathPredicate(
                        PathPredicate.newBuilder()
                            .setPathSegmentPredicate(
                                StringPredicate.newBuilder()
                                    .setValue("default-key")
                                    .setOperator(
                                        ai.traceable.external.data.classification.config.service.v1
                                            .Operator.OPERATOR_EQUALS))))
            .build();

    when(this.externalDataClassificationConfig.getDefaultExternalDataTypes())
        .thenReturn(List.of(defaultDataType));

    assertTrue(
        externalDataClassificationServiceBlockingStub
            .getDataClassificationConfig(GetDataClassificationConfigRequest.getDefaultInstance())
            .getDataTypesList()
            .contains(defaultDataType));
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
                      .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
                      .setSensitivity(Sensitivity.SENSITIVITY_LOW)
                      .addDataTypeIds("datatype-3"))
              .build();
      DataSet legacyDataSet =
          DataSet.newBuilder()
              .setId("legacy-dataset")
              .setInfo(
                  DataSetInfo.newBuilder()
                      .setName("legacy-dataset-1")
                      .setEnabled(false)
                      .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)
                      .setSensitivity(Sensitivity.SENSITIVITY_LOW)
                      .addDataTypeIds("datatype-3"))
              .build();
      responseObserver.onNext(
          GetDataSetsResponse.newBuilder()
              .addAllDataSets(List.of(dataSet1, dataSet2, dataSet3, legacyDataSet))
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
                              .setEnvironmentScope(
                                  EnvironmentScope.newBuilder().addEnvironmentIds("env-1"))
                              .addLocations(Location.LOCATION_ANY)
                              .setKeyPattern(
                                  StringPattern.newBuilder()
                                      .setValue("val-2")
                                      .setOperator(Operator.OPERATOR_EQUALS))
                              .setAction(Action.ACTION_MATCH)))
              .build();

      DataType dataType3 =
          DataType.newBuilder()
              .setId("datatype-3")
              .setRule(
                  DataTypeRule.newBuilder()
                      .setName("datatypename-3")
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

    @Override
    public void getDataClassificationOverrides(
        GetDataClassificationOverridesRequest request,
        StreamObserver<GetDataClassificationOverridesResponse> responseObserver) {
      responseObserver.onNext(
          GetDataClassificationOverridesResponse.newBuilder()
              .addAllDataClassificationOverrides(dataClassificationOverrides)
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

    @Override
    public void getRedactionStrategyForType(
        GetRedactionStrategyForTypeRequest request,
        StreamObserver<GetRedactionStrategyForTypeResponse> responseObserver) {
      responseObserver.onNext(
          GetRedactionStrategyForTypeResponse.newBuilder()
              .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_RAW)
              .build());
      responseObserver.onCompleted();
    }
  }
}
