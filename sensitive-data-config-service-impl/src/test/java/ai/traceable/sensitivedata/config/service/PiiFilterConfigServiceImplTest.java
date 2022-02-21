package ai.traceable.sensitivedata.config.service;

import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_REQUEST_BODY;
import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_REQUEST_HEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.MockInsightsService;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceImplBase;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Action;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.GlobalScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Operator;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.data.classification.config.service.v1.GetDataSetsRequest;
import ai.traceable.data.classification.config.service.v1.GetDataSetsResponse;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import ai.traceable.featureflag.v1.FeatureFlagServiceGrpc;
import ai.traceable.featureflag.v1.FeatureFlagServiceGrpc.FeatureFlagServiceImplBase;
import ai.traceable.featureflag.v1.FeatureFlagValue;
import ai.traceable.featureflag.v1.GetCurrentFlagValuesRequest;
import ai.traceable.featureflag.v1.GetCurrentFlagValuesResponse;
import ai.traceable.platform.insights.api.v1.InsightsServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.DropUnparsedJsonPolicy;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigResponse;
import ai.traceable.sensitivedata.config.service.v1.InvalidJsonPolicy;
import ai.traceable.sensitivedata.config.service.v1.MatchType;
import ai.traceable.sensitivedata.config.service.v1.NewRedactionRule;
import ai.traceable.sensitivedata.config.service.v1.PiiElement;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc.PiiFilterConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PiiFilterConfigServiceImplTest {

  PiiFilterConfigServiceBlockingStub piiFilterStub;
  MockGenericConfigService mockGenericConfigService;
  SensitiveDataServiceConfig mockConfig;
  UuidGenerator uuidGenerator = new UuidGenerator();
  boolean dataClassificationConfigServiceMockFlag;

  @BeforeEach
  void setUp() {
    mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGet().mockGetAll();

    mockConfig = mock(SensitiveDataServiceConfig.class);
    when(mockConfig.defaultParamTypeRedactionStrategy())
        .thenReturn(RedactionStrategy.REDACTION_STRATEGY_HASH);
    when(mockConfig.defaultPiiFilterConfig()).thenReturn(PiiFilterConfig.getDefaultInstance());
    when(mockConfig.defaultInvalidJsonPolicy())
        .thenReturn(
            InvalidJsonPolicy.newBuilder()
                .setDropUnparsedJsonPolicy(DropUnparsedJsonPolicy.getDefaultInstance())
                .build());
    when(mockConfig.getExpirationDuration()).thenReturn(Duration.ofMinutes(15));
    when(mockConfig.getRefreshDuration()).thenReturn(Duration.ofMinutes(5));
    when(mockConfig.getRequestTimeout()).thenReturn(Duration.ofSeconds(10));
    when(mockConfig.getThreadPoolSize()).thenReturn(1);
    dataClassificationConfigServiceMockFlag = false;
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void getPiiFilterConfig() {
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    GetPiiFilterConfigResponse response =
        piiFilterStub.getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build());

    PiiFilterConfig piiFilterConfig = response.getPiiFilterConfig();
    assertEquals(uuidGenerator.generateId(piiFilterConfig), response.getHash());
    assertTrue(piiFilterConfig.getInvalidJsonPolicy().hasDropUnparsedJsonPolicy());
    // Only the prepopulated rules + the ones in mock insights service
    Set<PiiElement> expected =
        Set.of(
            getPiiElement("abc123", RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("def456", RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement(
                "http.request.header.h1", RedactionStrategy.REDACTION_STRATEGY_HASH, true),
            getPiiElement(
                "http.request.header.h2", RedactionStrategy.REDACTION_STRATEGY_HASH, true));

    Set<PiiElement> actual =
        piiFilterConfig.getKeyRegexsList().stream()
            // Strip out rule ids for the prepopulated rules since they are random guids
            .map(piiElement -> PiiElement.newBuilder(piiElement).setRuleId("").build())
            .collect(Collectors.toSet());
    assertEquals(expected, actual);

    // no update
    response =
        piiFilterStub.getPiiFilterConfig(
            GetPiiFilterConfigRequest.newBuilder().setHash(response.getHash()).build());
    assertEquals(uuidGenerator.generateId(piiFilterConfig), response.getHash());
    assertEquals(
        GetPiiFilterConfigResponse.newBuilder()
            .setHash(uuidGenerator.generateId(piiFilterConfig))
            .build(),
        response);
  }

  @Test
  void getPiiFilterConfigFullPrivacyModeEnabled() {
    setupPiiFilterConfigServiceImpl(true, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // full privacy mode rules + prepopulated rules + mock insights service rules
    Set<PiiElement> expected =
        Set.of(
            getPiiElement("abc123", RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("def456", RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("uvw789", RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("xyz101112", RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement(
                "http.request.header.h1", RedactionStrategy.REDACTION_STRATEGY_HASH, true),
            getPiiElement(
                "http.request.header.h2", RedactionStrategy.REDACTION_STRATEGY_HASH, true));
    Set<PiiElement> actual =
        piiFilterConfig.getKeyRegexsList().stream()
            // Strip out rule ids for the prepopulated rules since they are random guids
            .map(piiElement -> PiiElement.newBuilder(piiElement).setRuleId("").build())
            .collect(Collectors.toSet());
    assertEquals(expected, actual);
  }

  @Test
  void prepopulatesFilterConfig() {
    setupPiiFilterConfigServiceImpl(false, mock(DefaultRedactionRules.class));

    when(mockConfig.defaultRedactionRules().isPrepopulationComplete(any())).thenReturn(false);
    when(mockConfig.defaultRedactionRules().completedPrepopulationStatus(any()))
        .thenReturn(DefaultRedactionRulePopulationStatus.of(Set.of("key")));
    when(mockConfig.defaultRedactionRules().getRulesToPrepopulate(any()))
        .thenReturn(
            Map.of(
                "other-key",
                NewRedactionRule.newBuilder()
                    .setMatchType(MatchType.MATCH_TYPE_KEY)
                    .setRegex("prepopulated-regex")
                    .build()));

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();

    assertTrue(
        piiFilterConfig.getKeyRegexsList().stream()
            .anyMatch(element -> element.getRegex().equals("prepopulated-regex")));
  }

  @Test
  void testPiiFilterConfigForDataTypes() {
    dataClassificationConfigServiceMockFlag = true;
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // prepopulated rules + mock insights service rules + rules from datatypes in datasets
    Set<PiiElement> expected =
        Set.of(
            getPiiElement("abc123", RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("def456", RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement(
                "http.request.header.h1", RedactionStrategy.REDACTION_STRATEGY_HASH, true),
            getPiiElement(
                "http.request.header.h2", RedactionStrategy.REDACTION_STRATEGY_HASH, true),
            getPiiElement(
                "http.request.header.regex-1", RedactionStrategy.REDACTION_STRATEGY_REDACT, true),
            getPiiElement(
                "rpc.request.metadata.regex-1", RedactionStrategy.REDACTION_STRATEGY_REDACT, true),
            getPiiElement("regex-2", RedactionStrategy.REDACTION_STRATEGY_REDACT, false));
    Set<PiiElement> actual =
        piiFilterConfig.getKeyRegexsList().stream()
            // Strip out rule ids for the prepopulated rules since they are random guids
            .map(piiElement -> PiiElement.newBuilder(piiElement).setRuleId("").build())
            .collect(Collectors.toSet());
    assertEquals(expected, actual);
  }

  private PiiElement getPiiElement(String name, RedactionStrategy redactionStrategy, boolean fqn) {
    return PiiElement.newBuilder()
        .setRegex(name)
        .setRedactionStrategy(redactionStrategy)
        .setFqn(fqn)
        .build();
  }

  private DefaultRedactionRules getDefaultRedactionRules() {
    // prepopulated rules
    Map<String, NewRedactionRule> prepopulationRules =
        Map.of(
            "pre-populated-1",
            NewRedactionRule.newBuilder()
                .setName("Prepopulated Rule 1")
                .setMatchType(MatchType.MATCH_TYPE_KEY)
                .setRegex("abc123")
                .build(),
            "pre-populated-2",
            NewRedactionRule.newBuilder()
                .setName("Prepopulated Rule 2")
                .setMatchType(MatchType.MATCH_TYPE_HEADER)
                .setRegex("def456")
                .build());
    // full privacy mode rules
    List<RedactionRule> defaultRedactionRules =
        List.of(
            RedactionRule.newBuilder()
                .setName("Rule 1")
                .setMatchType(MatchType.MATCH_TYPE_KEY)
                .setRegex("uvw789")
                .build(),
            RedactionRule.newBuilder()
                .setName("Rule 2")
                .setMatchType(MatchType.MATCH_TYPE_KEY)
                .setRegex("xyz101112")
                .build());

    return new DefaultRedactionRules(prepopulationRules, defaultRedactionRules);
  }

  private void setupPiiFilterConfigServiceImpl(
      boolean defaultFullPrivacyMode, DefaultRedactionRules defaultRedactionRules) {
    Channel channel = mockGenericConfigService.channel();
    when(mockConfig.defaultFullPrivacyMode()).thenReturn(defaultFullPrivacyMode);
    when(mockConfig.defaultRedactionRules()).thenReturn(defaultRedactionRules);

    ConfigChangeEventGenerator configChangeEventGenerator = mock(ConfigChangeEventGenerator.class);
    ConfigServiceBlockingStub configServiceBlockingStub =
        ConfigServiceGrpc.newBlockingStub(channel);
    mockGenericConfigService
        .addService(new MockInsightsService())
        .addService(
            new PiiFilterConfigServiceImpl(
                mockConfig,
                new ConfigServiceCoordinatorImpl(
                    configServiceBlockingStub,
                    configChangeEventGenerator,
                    mockConfig,
                    new RedactionRuleConfigStore(
                        configServiceBlockingStub, configChangeEventGenerator),
                    new AutomaticSecretRedactionStrategyConfigStore(
                        configServiceBlockingStub, configChangeEventGenerator),
                    new InvalidJsonPolicyConfigStore(
                        configServiceBlockingStub, configChangeEventGenerator),
                    new FullPrivacyModeConfigStore(
                        configServiceBlockingStub, configChangeEventGenerator),
                    new DefaultRedactionRulePopulationStatusStore(
                        configServiceBlockingStub, configChangeEventGenerator),
                    DataClassificationConfigServiceGrpc.newBlockingStub(channel),
                    FeatureFlagServiceGrpc.newBlockingStub(channel)),
                new InsightsServiceCoordinatorImpl(InsightsServiceGrpc.newBlockingStub(channel)),
                new UuidGenerator()))
        .addService(new MockDataClassificationConfigService())
        .addService(new MockFeatureFlagService())
        .start();

    piiFilterStub = PiiFilterConfigServiceGrpc.newBlockingStub(channel);
  }

  class MockDataClassificationConfigService extends DataClassificationConfigServiceImplBase {

    @Override
    public void getDataSets(
        GetDataSetsRequest request, StreamObserver<GetDataSetsResponse> responseObserver) {
      GetDataSetsResponse.Builder responseBuilder = GetDataSetsResponse.newBuilder();
      if (dataClassificationConfigServiceMockFlag) {
        DataSet dataSet =
            DataSet.newBuilder()
                .setInfo(
                    DataSetInfo.newBuilder()
                        .setName("dataset-1")
                        .setEnabled(true)
                        .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
                        .addAllDataTypeIds(List.of("datatype-1", "datatype-2")))
                .build();
        responseBuilder.addDataSets(dataSet);
      }
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    }

    @Override
    public void getDataTypes(
        GetDataTypesRequest request, StreamObserver<GetDataTypesResponse> responseObserver) {
      GetDataTypesResponse.Builder responseBuilder = GetDataTypesResponse.newBuilder();
      if (dataClassificationConfigServiceMockFlag) {
        DataType dataType1 =
            DataType.newBuilder()
                .setId("datatype-1")
                .setRule(
                    DataTypeRule.newBuilder()
                        .setName("datatyperule-1")
                        .addScopedPatterns(
                            ScopedPattern.newBuilder()
                                .setGlobalScope(GlobalScope.getDefaultInstance())
                                .addLocations(LOCATION_REQUEST_HEADER)
                                .setKeyPattern(
                                    StringPattern.newBuilder()
                                        .setValue("regex-1")
                                        .setOperator(Operator.OPERATOR_MATCHES_REGEX))
                                .setAction(Action.ACTION_MATCH)))
                .build();
        DataType dataType2 =
            DataType.newBuilder()
                .setId("datatype-2")
                .setRule(
                    DataTypeRule.newBuilder()
                        .setName("datatyperule-2")
                        .addScopedPatterns(
                            ScopedPattern.newBuilder()
                                .setGlobalScope(GlobalScope.getDefaultInstance())
                                .addLocations(LOCATION_REQUEST_BODY)
                                .setKeyPattern(
                                    StringPattern.newBuilder()
                                        .setValue("regex-2")
                                        .setOperator(Operator.OPERATOR_MATCHES_REGEX))
                                .setAction(Action.ACTION_MATCH)))
                .build();
        responseBuilder.addAllDataTypes(List.of(dataType1, dataType2));
      }
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    }
  }

  class MockFeatureFlagService extends FeatureFlagServiceImplBase {
    @Override
    public void getCurrentFlagValues(
        GetCurrentFlagValuesRequest request,
        StreamObserver<GetCurrentFlagValuesResponse> responseObserver) {
      responseObserver.onNext(
          GetCurrentFlagValuesResponse.newBuilder()
              .putValues(
                  "data-classification.mvp", FeatureFlagValue.newBuilder().setBoolean(true).build())
              .build());
      responseObserver.onCompleted();
    }
  }
}
