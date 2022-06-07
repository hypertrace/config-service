package ai.traceable.sensitivedata.config.service;

import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_REQUEST_BODY;
import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_REQUEST_HEADER;
import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID;
import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.LEGACY_OBFUSCATE_DATA_SET_ID;
import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.LEGACY_REDACT_DATA_SET_ID;
import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.LEGACY_SENSITIVE_HEADERS_DATA_SET_ID;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_HASH;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_RAW;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_REDACT;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.MockInsightsService;
import ai.traceable.config.service.feature.caching.client.FeatureCachingClient;
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
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
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
  FeatureCachingClient mockFeatureClient;
  boolean dataClassificationRp1Enabled;
  boolean customDataSetEnabled;
  List<DataSet> legacyDataSets;

  @BeforeEach
  void setUp() {
    mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGet().mockGetAll();

    mockConfig = mock(SensitiveDataServiceConfig.class);
    mockFeatureClient = mock(FeatureCachingClient.class);
    when(mockConfig.defaultParamTypeRedactionStrategy()).thenReturn(REDACTION_STRATEGY_HASH);
    when(mockConfig.defaultAutomaticRedactionStrategy()).thenReturn(true);
    when(mockConfig.defaultPiiFilterConfig())
        .thenReturn(
            PiiFilterConfig.newBuilder()
                .addKeyRegexs(PiiElement.newBuilder().setRegex("secret-1"))
                .build());
    when(mockConfig.defaultInvalidJsonPolicy())
        .thenReturn(
            InvalidJsonPolicy.newBuilder()
                .setDropUnparsedJsonPolicy(DropUnparsedJsonPolicy.getDefaultInstance())
                .build());
    dataClassificationRp1Enabled = false;
    customDataSetEnabled = true;
    legacyDataSets = Collections.emptyList();
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
            getPiiElement("secret-1", REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("session-id", REDACTION_STRATEGY_HASH, false, true),
            getPiiElement("abc123", REDACTION_STRATEGY_REDACT, false),
            getPiiElement("def456", REDACTION_STRATEGY_HASH, false),
            getPiiElement("ghi789", REDACTION_STRATEGY_RAW, false),
            getPiiElement("http.request.header.h1", REDACTION_STRATEGY_HASH, true),
            getPiiElement("http.request.header.h2", REDACTION_STRATEGY_HASH, true));

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
            .setEnabled(true)
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
            getPiiElement("secret-1", REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("session-id", REDACTION_STRATEGY_HASH, false, true),
            getPiiElement("abc123", REDACTION_STRATEGY_REDACT, false),
            getPiiElement("def456", REDACTION_STRATEGY_HASH, false),
            getPiiElement("ghi789", REDACTION_STRATEGY_RAW, false),
            getPiiElement("http.request.header.h1", REDACTION_STRATEGY_HASH, true),
            getPiiElement("http.request.header.h2", REDACTION_STRATEGY_HASH, true));
    Set<PiiElement> actual =
        piiFilterConfig.getKeyRegexsList().stream()
            // Strip out rule ids for the prepopulated rules since they are random guids
            .map(piiElement -> PiiElement.newBuilder(piiElement).setRuleId("").build())
            .collect(Collectors.toSet());
    assertEquals(expected, actual);
  }

  @Test
  void testPiiFilterConfigForDataTypes() {
    dataClassificationRp1Enabled = true;
    legacyDataSets =
        List.of(
            createDataSet(LEGACY_REDACT_DATA_SET_ID, true),
            createDataSet(LEGACY_OBFUSCATE_DATA_SET_ID, true),
            createDataSet(LEGACY_SENSITIVE_HEADERS_DATA_SET_ID, true),
            createDataSet(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID, true));
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // prepopulated rules + mock insights service rules + rules from datatypes in datasets
    Set<PiiElement> expected =
        Set.of(
            getPiiElement("secret-1", REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("session-id", REDACTION_STRATEGY_HASH, false, true),
            getPiiElement("abc123", REDACTION_STRATEGY_REDACT, false),
            getPiiElement("def456", REDACTION_STRATEGY_HASH, false),
            getPiiElement("http.request.header.h1", REDACTION_STRATEGY_HASH, true),
            getPiiElement("http.request.header.h2", REDACTION_STRATEGY_HASH, true),
            getPiiElement("http.request.header.regex-1", REDACTION_STRATEGY_REDACT, true),
            getPiiElement("rpc.request.metadata.regex-1", REDACTION_STRATEGY_REDACT, true),
            getPiiElement("regex-2", REDACTION_STRATEGY_REDACT, false));
    Set<PiiElement> actual =
        piiFilterConfig.getKeyRegexsList().stream()
            // Strip out rule ids for the prepopulated rules since they are random guids
            .map(piiElement -> PiiElement.newBuilder(piiElement).setRuleId("").build())
            .collect(Collectors.toSet());
    assertEquals(expected, actual);
  }

  @Test
  void testPiiFilterConfigForDataTypesWithoutLegacyDataSets() {
    dataClassificationRp1Enabled = true;
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // prepopulated rules + mock insights service rules + rules from datatypes in datasets
    Set<PiiElement> expected =
        Set.of(
            getPiiElement("session-id", REDACTION_STRATEGY_HASH, false, true),
            getPiiElement("http.request.header.regex-1", REDACTION_STRATEGY_REDACT, true),
            getPiiElement("rpc.request.metadata.regex-1", REDACTION_STRATEGY_REDACT, true),
            getPiiElement("regex-2", REDACTION_STRATEGY_REDACT, false));
    Set<PiiElement> actual =
        piiFilterConfig.getKeyRegexsList().stream()
            // Strip out rule ids for the prepopulated rules since they are random guids
            .map(piiElement -> PiiElement.newBuilder(piiElement).setRuleId("").build())
            .collect(Collectors.toSet());
    assertEquals(expected, actual);
  }

  @Test
  void testPiiFilterConfigForDataTypesWithoutCustomDataSets() {
    dataClassificationRp1Enabled = true;
    customDataSetEnabled = false;
    legacyDataSets =
        List.of(
            createDataSet(LEGACY_REDACT_DATA_SET_ID, true),
            createDataSet(LEGACY_OBFUSCATE_DATA_SET_ID, true),
            createDataSet(LEGACY_SENSITIVE_HEADERS_DATA_SET_ID, true),
            createDataSet(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID, true));
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // prepopulated rules + mock insights service rules + rules from datatypes in datasets
    Set<PiiElement> expected =
        Set.of(
            getPiiElement("secret-1", REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("session-id", REDACTION_STRATEGY_HASH, false, true),
            getPiiElement("abc123", REDACTION_STRATEGY_REDACT, false),
            getPiiElement("def456", REDACTION_STRATEGY_HASH, false),
            getPiiElement("http.request.header.h1", REDACTION_STRATEGY_HASH, true),
            getPiiElement("http.request.header.h2", REDACTION_STRATEGY_HASH, true));
    Set<PiiElement> actual =
        piiFilterConfig.getKeyRegexsList().stream()
            // Strip out rule ids for the prepopulated rules since they are random guids
            .map(piiElement -> PiiElement.newBuilder(piiElement).setRuleId("").build())
            .collect(Collectors.toSet());
    assertEquals(expected, actual);
  }

  @Test
  void testPiiFilterConfigForDataTypesWithoutSensitiveHeadersDataSet() {
    dataClassificationRp1Enabled = true;
    legacyDataSets =
        List.of(
            createDataSet(LEGACY_REDACT_DATA_SET_ID, true),
            createDataSet(LEGACY_OBFUSCATE_DATA_SET_ID, true),
            createDataSet(LEGACY_SENSITIVE_HEADERS_DATA_SET_ID, false),
            createDataSet(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID, true));
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // prepopulated rules + mock insights service rules + rules from datatypes in datasets
    Set<PiiElement> expected =
        Set.of(
            getPiiElement("secret-1", REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("session-id", REDACTION_STRATEGY_HASH, false, true),
            getPiiElement("abc123", REDACTION_STRATEGY_REDACT, false),
            getPiiElement("def456", REDACTION_STRATEGY_HASH, false),
            getPiiElement("http.request.header.regex-1", REDACTION_STRATEGY_REDACT, true),
            getPiiElement("rpc.request.metadata.regex-1", REDACTION_STRATEGY_REDACT, true),
            getPiiElement("regex-2", REDACTION_STRATEGY_REDACT, false));
    Set<PiiElement> actual =
        piiFilterConfig.getKeyRegexsList().stream()
            // Strip out rule ids for the prepopulated rules since they are random guids
            .map(piiElement -> PiiElement.newBuilder(piiElement).setRuleId("").build())
            .collect(Collectors.toSet());
    assertEquals(expected, actual);
  }

  @Test
  void testPiiFilterConfigForDataTypesWithoutAutoSecretRedactionDataSet() {
    dataClassificationRp1Enabled = true;
    legacyDataSets =
        List.of(
            createDataSet(LEGACY_REDACT_DATA_SET_ID, true),
            createDataSet(LEGACY_OBFUSCATE_DATA_SET_ID, true),
            createDataSet(LEGACY_SENSITIVE_HEADERS_DATA_SET_ID, true),
            createDataSet(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID, false));
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // prepopulated rules + mock insights service rules + rules from datatypes in datasets
    Set<PiiElement> expected =
        Set.of(
            getPiiElement("session-id", REDACTION_STRATEGY_HASH, false, true),
            getPiiElement("abc123", REDACTION_STRATEGY_REDACT, false),
            getPiiElement("def456", REDACTION_STRATEGY_HASH, false),
            getPiiElement("http.request.header.h1", REDACTION_STRATEGY_HASH, true),
            getPiiElement("http.request.header.h2", REDACTION_STRATEGY_HASH, true),
            getPiiElement("http.request.header.regex-1", REDACTION_STRATEGY_REDACT, true),
            getPiiElement("rpc.request.metadata.regex-1", REDACTION_STRATEGY_REDACT, true),
            getPiiElement("regex-2", REDACTION_STRATEGY_REDACT, false));
    Set<PiiElement> actual =
        piiFilterConfig.getKeyRegexsList().stream()
            // Strip out rule ids for the prepopulated rules since they are random guids
            .map(piiElement -> PiiElement.newBuilder(piiElement).setRuleId("").build())
            .collect(Collectors.toSet());
    assertEquals(expected, actual);
  }

  @Test
  void testPiiFilterConfigForDataTypesWithoutRedactDataSet() {
    dataClassificationRp1Enabled = true;
    legacyDataSets =
        List.of(
            createDataSet(LEGACY_REDACT_DATA_SET_ID, false),
            createDataSet(LEGACY_OBFUSCATE_DATA_SET_ID, true),
            createDataSet(LEGACY_SENSITIVE_HEADERS_DATA_SET_ID, true),
            createDataSet(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID, true));
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // prepopulated rules + mock insights service rules + rules from datatypes in datasets
    Set<PiiElement> expected =
        Set.of(
            getPiiElement("secret-1", REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("session-id", REDACTION_STRATEGY_HASH, false, true),
            getPiiElement("def456", REDACTION_STRATEGY_HASH, false),
            getPiiElement("http.request.header.h1", REDACTION_STRATEGY_HASH, true),
            getPiiElement("http.request.header.h2", REDACTION_STRATEGY_HASH, true),
            getPiiElement("http.request.header.regex-1", REDACTION_STRATEGY_REDACT, true),
            getPiiElement("rpc.request.metadata.regex-1", REDACTION_STRATEGY_REDACT, true),
            getPiiElement("regex-2", REDACTION_STRATEGY_REDACT, false));
    Set<PiiElement> actual =
        piiFilterConfig.getKeyRegexsList().stream()
            // Strip out rule ids for the prepopulated rules since they are random guids
            .map(piiElement -> PiiElement.newBuilder(piiElement).setRuleId("").build())
            .collect(Collectors.toSet());
    assertEquals(expected, actual);
  }

  @Test
  void testPiiFilterConfigForDataTypesWithoutObfuscateDataSet() {
    dataClassificationRp1Enabled = true;
    legacyDataSets =
        List.of(
            createDataSet(LEGACY_REDACT_DATA_SET_ID, true),
            createDataSet(LEGACY_OBFUSCATE_DATA_SET_ID, false),
            createDataSet(LEGACY_SENSITIVE_HEADERS_DATA_SET_ID, true),
            createDataSet(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_SET_ID, true));
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // prepopulated rules + mock insights service rules + rules from datatypes in datasets
    Set<PiiElement> expected =
        Set.of(
            getPiiElement("secret-1", REDACTION_STRATEGY_UNSPECIFIED, false),
            getPiiElement("session-id", REDACTION_STRATEGY_HASH, false, true),
            getPiiElement("abc123", REDACTION_STRATEGY_REDACT, false),
            getPiiElement("http.request.header.h1", REDACTION_STRATEGY_HASH, true),
            getPiiElement("http.request.header.h2", REDACTION_STRATEGY_HASH, true),
            getPiiElement("http.request.header.regex-1", REDACTION_STRATEGY_REDACT, true),
            getPiiElement("rpc.request.metadata.regex-1", REDACTION_STRATEGY_REDACT, true),
            getPiiElement("regex-2", REDACTION_STRATEGY_REDACT, false));
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

  private PiiElement getPiiElement(
      String name, RedactionStrategy redactionStrategy, boolean fqn, boolean sessionIdentifier) {
    return PiiElement.newBuilder()
        .setRegex(name)
        .setRedactionStrategy(redactionStrategy)
        .setFqn(fqn)
        .setSessionIdentifier(sessionIdentifier)
        .build();
  }

  private DefaultRedactionRules getDefaultRedactionRules() {
    // prepopulated rules
    Map<String, NewRedactionRule> defaultRules =
        Map.of(
            "default-1",
            NewRedactionRule.newBuilder()
                .setName("Rule 1")
                .setMatchType(MatchType.MATCH_TYPE_KEY)
                .setRegex("abc123")
                .setRedactionStrategy(REDACTION_STRATEGY_REDACT)
                .build(),
            "default-2",
            NewRedactionRule.newBuilder()
                .setName("Rule 2")
                .setMatchType(MatchType.MATCH_TYPE_HEADER)
                .setRegex("def456")
                .setRedactionStrategy(REDACTION_STRATEGY_HASH)
                .build(),
            "default-3",
            NewRedactionRule.newBuilder()
                .setName("Rule 3")
                .setMatchType(MatchType.MATCH_TYPE_HEADER)
                .setRegex("ghi789")
                .setRedactionStrategy(REDACTION_STRATEGY_RAW)
                .build(),
            "default-session-id-1",
            NewRedactionRule.newBuilder()
                .setName("Rule 4")
                .setMatchType(MatchType.MATCH_TYPE_HEADER)
                .setRegex("session-id")
                .setRedactionStrategy(REDACTION_STRATEGY_HASH)
                .setSessionIdentifier(true)
                .build());

    return new DefaultRedactionRules(defaultRules);
  }

  private DataSet createDataSet(String dataSetId, boolean enabled) {
    return DataSet.newBuilder()
        .setId(dataSetId)
        .setInfo(DataSetInfo.newBuilder().setEnabled(enabled))
        .build();
  }

  private void setupPiiFilterConfigServiceImpl(
      boolean defaultFullPrivacyMode, DefaultRedactionRules defaultRedactionRules) {
    Channel channel = mockGenericConfigService.channel();
    when(mockConfig.defaultFullPrivacyMode()).thenReturn(defaultFullPrivacyMode);
    when(mockConfig.defaultRedactionRules()).thenReturn(defaultRedactionRules);

    when(mockFeatureClient.isDataClassificationRp1Enabled(any()))
        .thenAnswer(unused -> this.dataClassificationRp1Enabled);
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
                    new DefaultRedactionRulePersistenceStatusStore(
                        configServiceBlockingStub, configChangeEventGenerator),
                    DataClassificationConfigServiceGrpc.newBlockingStub(channel)),
                new InsightsServiceCoordinatorImpl(InsightsServiceGrpc.newBlockingStub(channel)),
                new UuidGenerator(),
                mockFeatureClient))
        .addService(new MockDataClassificationConfigService())
        .start();

    piiFilterStub = PiiFilterConfigServiceGrpc.newBlockingStub(channel);
  }

  class MockDataClassificationConfigService extends DataClassificationConfigServiceImplBase {

    @Override
    public void getDataSets(
        GetDataSetsRequest request, StreamObserver<GetDataSetsResponse> responseObserver) {
      GetDataSetsResponse.Builder responseBuilder = GetDataSetsResponse.newBuilder();
      DataSet dataSet =
          DataSet.newBuilder()
              .setInfo(
                  DataSetInfo.newBuilder()
                      .setName("dataset-1")
                      .setEnabled(customDataSetEnabled)
                      .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
                      .addAllDataTypeIds(List.of("datatype-1", "datatype-2")))
              .build();
      responseBuilder.addDataSets(dataSet);
      responseBuilder.addAllDataSets(legacyDataSets);
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    }

    @Override
    public void getDataTypes(
        GetDataTypesRequest request, StreamObserver<GetDataTypesResponse> responseObserver) {
      GetDataTypesResponse.Builder responseBuilder = GetDataTypesResponse.newBuilder();
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
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    }
  }
}
