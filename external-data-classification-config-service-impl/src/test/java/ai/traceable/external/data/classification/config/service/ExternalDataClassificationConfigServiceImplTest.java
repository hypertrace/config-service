package ai.traceable.external.data.classification.config.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverride;
import ai.traceable.data.classification.config.service.v1.DataClassificationOverrideRule;
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
import ai.traceable.platform.insights.api.v1.InsightsServiceGrpc;
import ai.traceable.platform.insights.api.v1.InsightsServiceGrpc.InsightsServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesResponse;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeResponse;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import ai.traceable.sessionidentification.config.service.v1.GetSessionIdentificationRulesRequest;
import ai.traceable.sessionidentification.config.service.v1.GetSessionIdentificationRulesResponse;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationConfigServiceGrpc;
import ai.traceable.sessionidentification.config.service.v1.SessionIdentificationConfigServiceGrpc.SessionIdentificationConfigServiceBlockingStub;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import com.typesafe.config.ConfigFactory;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.core.grpcutils.client.InProcessGrpcChannelRegistry;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ExternalDataClassificationConfigServiceImplTest {
  private static final ai.traceable.external.data.classification.config.service.v1.DataType
      EXPECTED_SESSION_ID_DATA_TYPE =
          ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
              .setDataTypeId("old-session-rule")
              .addMatchRules(
                  DataTypeMatchRule.newBuilder()
                      .setResult(Result.RESULT_MATCH)
                      .setPathPredicate(
                          PathPredicate.newBuilder()
                              .setPathSegmentPredicate(
                                  StringPredicate.newBuilder()
                                      .setOperator(
                                          ai.traceable.external.data.classification.config.service
                                              .v1.Operator.OPERATOR_MATCHES_REGEX)
                                      .setValue("session"))))
              .setSessionIdentifier(true)
              .build();
  MockGenericConfigService mockGenericConfigService;
  ExternalDataClassificationServiceBlockingStub externalDataClassificationServiceBlockingStub;
  @Mock FeatureCachingClient featureCachingClient;
  List<DataClassificationOverride> dataClassificationOverrides;
  @Mock ExternalDataClassificationConfig externalDataClassificationConfig;

  @BeforeEach
  void setup() {
    PlatformMetricsRegistry.getMeterRegistry().clear();
    mockGenericConfigService = new MockGenericConfigService();
    Injector injector =
        Guice.createInjector(
            Modules.override(
                    new ExternalDataClassificationConfigServiceModule(
                        this.mockGenericConfigService.channel(),
                        ConfigFactory.empty(),
                        new InProcessGrpcChannelRegistry(),
                        this.featureCachingClient))
                .with(
                    new AbstractModule() {
                      @Override
                      public void configure() {
                        bind(ExternalDataClassificationConfig.class)
                            .toInstance(externalDataClassificationConfig);
                        bind(SensitiveDataConfigServiceBlockingStub.class)
                            .toInstance(
                                SensitiveDataConfigServiceGrpc.newBlockingStub(
                                    mockGenericConfigService.channel()));
                        bind(DataClassificationConfigServiceBlockingStub.class)
                            .toInstance(
                                DataClassificationConfigServiceGrpc.newBlockingStub(
                                    mockGenericConfigService.channel()));
                        bind(SessionIdentificationConfigServiceBlockingStub.class)
                            .toInstance(
                                SessionIdentificationConfigServiceGrpc.newBlockingStub(
                                    mockGenericConfigService.channel()));
                        bind(InsightsServiceBlockingStub.class)
                            .toInstance(
                                InsightsServiceGrpc.newBlockingStub(
                                    mockGenericConfigService.channel()));
                      }
                    }));
    when(externalDataClassificationConfig.getCacheRefreshDuration())
        .thenReturn(Duration.ofMinutes(1));
    when(externalDataClassificationConfig.getCacheThreadPoolSize()).thenReturn(1);
    when(externalDataClassificationConfig.getCacheMaxSize()).thenReturn(10);
    when(externalDataClassificationConfig.getCacheExpirationDuration())
        .thenReturn(Duration.ofMinutes(1));
    mockGenericConfigService
        .addService(new MockSessionIdentificationConfigService())
        .addService(injector.getInstance(BindableService.class))
        .addService(new MockSensitiveDataConfigService())
        .addService(new MockDataClassificationConfigService())
        .start();
    externalDataClassificationServiceBlockingStub =
        ExternalDataClassificationServiceGrpc.newBlockingStub(
                this.mockGenericConfigService.channel())
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
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
    assertEquals(2, response.getDataTypesCount());

    response =
        externalDataClassificationServiceBlockingStub.getDataClassificationConfig(
            GetDataClassificationConfigRequest.newBuilder()
                .setChangeFilter(OnlyIfChangedFilter.getDefaultInstance())
                .setEnvironmentFilter(EnvironmentFilter.newBuilder().setEnvironmentName("env-1"))
                .build());
    assertEquals(4, response.getDataTypesCount());
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
    assertEquals(List.of(EXPECTED_SESSION_ID_DATA_TYPE), response.getDataTypesList());
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

  @Test
  void updatesDefaultRuleIfOverriddenSuppression() {
    this.dataClassificationOverrides =
        List.of(
            DataClassificationOverride.newBuilder()
                .setDataClassificationOverrideRule(
                    DataClassificationOverrideRule.newBuilder()
                        .setDataSuppressionOverride(
                            DataClassificationOverrideRule.DataSuppressionOverride.newBuilder()
                                .setDataSuppression(DataSuppression.DATA_SUPPRESSION_RAW)))
                .build());
    when(this.externalDataClassificationConfig.getDefaultExternalDataTypes())
        .thenReturn(
            List.of(
                ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
                    .setDataTypeId("default-rule")
                    .setTransformation(
                        ai.traceable.external.data.classification.config.service.v1.DataType
                            .DataTransformation.DATA_TRANSFORMATION_REDACT)
                    .build()));
    assertEquals(
        List.of(
            EXPECTED_SESSION_ID_DATA_TYPE,
            ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
                .setDataTypeId("default-rule")
                .build()),
        externalDataClassificationServiceBlockingStub
            .getDataClassificationConfig(
                GetDataClassificationConfigRequest.newBuilder()
                    .setEnvironmentFilter(
                        EnvironmentFilter.newBuilder().setEnvironmentName("random"))
                    .build())
            .getDataTypesList());
  }

  @Test
  void omitDbStatementDataTypeIfRASPIsEnabled() {
    when(featureCachingClient.isRaspInspectionEnabled(any())).thenReturn(true);
    dataClassificationOverrides = new ArrayList<>();
    ai.traceable.external.data.classification.config.service.v1.DataType defaultDataType =
        ai.traceable.external.data.classification.config.service.v1.DataType.newBuilder()
            .setDataTypeId("db_query_attributes")
            .setTransformation(
                ai.traceable.external.data.classification.config.service.v1.DataType
                    .DataTransformation.DATA_TRANSFORMATION_REDACT)
            .build();

    when(this.externalDataClassificationConfig.getDefaultExternalDataTypes())
        .thenReturn(List.of(defaultDataType));

    Optional<ai.traceable.external.data.classification.config.service.v1.DataType>
        dbStatementDataType =
            RequestContext.forTenantId("First")
                .call(
                    () ->
                        externalDataClassificationServiceBlockingStub
                            .getDataClassificationConfig(
                                GetDataClassificationConfigRequest.getDefaultInstance())
                            .getDataTypesList()
                            .stream()
                            .filter(
                                dataType -> "db_query_attributes".equals(dataType.getDataTypeId()))
                            .findFirst());

    assertEquals(
        Optional.of(defaultDataType.toBuilder().clearTransformation().build()),
        dbStatementDataType);

    when(featureCachingClient.isRaspInspectionEnabled(any())).thenReturn(false);
    // Use new request context to ensure we don't hit the cached result from first FF check
    dbStatementDataType =
        RequestContext.forTenantId("Second")
            .call(
                () ->
                    externalDataClassificationServiceBlockingStub
                        .getDataClassificationConfig(
                            GetDataClassificationConfigRequest.getDefaultInstance())
                        .getDataTypesList()
                        .stream()
                        .filter(dataType -> "db_query_attributes".equals(dataType.getDataTypeId()))
                        .findFirst());

    assertEquals(Optional.of(defaultDataType), dbStatementDataType);
  }

  @Test
  void cachesNextResponse() {
    dataClassificationOverrides = new ArrayList<>();
    GetDataClassificationConfigResponse firstResponse =
        externalDataClassificationServiceBlockingStub.getDataClassificationConfig(
            GetDataClassificationConfigRequest.getDefaultInstance());
    // Same request again (which wouldn't typically happen from the same agent)
    externalDataClassificationServiceBlockingStub.getDataClassificationConfig(
        GetDataClassificationConfigRequest.getDefaultInstance());
    // New request with newly generated hash

    GetDataClassificationConfigResponse secondResponse =
        externalDataClassificationServiceBlockingStub.getDataClassificationConfig(
            GetDataClassificationConfigRequest.newBuilder()
                .setChangeFilter(
                    OnlyIfChangedFilter.newBuilder().setPreviousHash(firstResponse.getHash()))
                .build());

    // Second response should have same hash, but otherwise be empty
    assertEquals(firstResponse.getHash(), secondResponse.getHash());
    assertEquals(
        GetDataClassificationConfigResponse.getDefaultInstance(),
        secondResponse.toBuilder().clearHash().build());

    assertEquals(
        2,
        PlatformMetricsRegistry.getMeterRegistry()
            .get("cache.gets")
            .tag("cache", "ExternalDataClassificationConfigServiceImplResponseCache")
            .tag("result", "hit")
            .functionCounter()
            .count());
    assertEquals(
        1,
        PlatformMetricsRegistry.getMeterRegistry()
            .get("cache.gets")
            .tag("cache", "ExternalDataClassificationConfigServiceImplResponseCache")
            .tag("result", "miss")
            .functionCounter()
            .count());
  }

  class MockDataClassificationConfigService
      extends DataClassificationConfigServiceGrpc.DataClassificationConfigServiceImplBase {

    @Override
    public void getDataTypes(
        GetDataTypesRequest request, StreamObserver<GetDataTypesResponse> responseObserver) {
      DataType dataType1 =
          DataType.newBuilder()
              .setId("datatype-1")
              .setRule(
                  DataTypeRule.newBuilder()
                      .setName("datatypename-1")
                      .setEnabled(true)
                      .setDataSuppression(DataSuppression.DATA_SUPPRESSION_OBFUSCATE)
                      .setSensitivity(Sensitivity.SENSITIVITY_HIGH)
                      .addDataSetId("dataset-2")
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
                      .setEnabled(true)
                      .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
                      .setSensitivity(Sensitivity.SENSITIVITY_MEDIUM)
                      .addDataSetId("dataset-1")
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
                      .setEnabled(true)
                      .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
                      .setSensitivity(Sensitivity.SENSITIVITY_LOW)
                      .addDataSetId("dataset-3")
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
              .addAllDataTypes(List.of(dataType2, dataType3, dataType1))
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

  static class MockSensitiveDataConfigService
      extends SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceImplBase {

    @Override
    public void getAllRedactionRules(
        GetAllRedactionRulesRequest request,
        StreamObserver<GetAllRedactionRulesResponse> responseObserver) {
      GetAllRedactionRulesResponse.Builder responseBuilder =
          GetAllRedactionRulesResponse.newBuilder()
              .addRedactionRules(
                  RedactionRule.newBuilder()
                      .setRedactionStrategy(RedactionStrategy.REDACTION_STRATEGY_RAW)
                      .setId("old-session-rule")
                      .setRegex("session")
                      .setSessionIdentifier(true));
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

  static class MockSessionIdentificationConfigService
      extends SessionIdentificationConfigServiceGrpc.SessionIdentificationConfigServiceImplBase {

    @Override
    public void getSessionIdentificationRules(
        GetSessionIdentificationRulesRequest request,
        StreamObserver<GetSessionIdentificationRulesResponse> responseObserver) {
      GetSessionIdentificationRulesResponse.Builder responseBuilder =
          GetSessionIdentificationRulesResponse.newBuilder();
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    }
  }
}
