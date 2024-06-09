package ai.traceable.sensitivedata.config.service;

import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_REQUEST_BODY;
import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_REQUEST_HEADER;
import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE_ID;
import static ai.traceable.sensitivedata.config.service.PiiFilterConfigServiceImpl.LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_HASH;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_RAW;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_REDACT;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_UNSPECIFIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.traceable.config.service.MockInsightsService;
import ai.traceable.config.utils.UuidGenerator;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceImplBase;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Action;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.GlobalScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Operator;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.data.classification.config.service.v1.GetDataTypesRequest;
import ai.traceable.data.classification.config.service.v1.GetDataTypesResponse;
import ai.traceable.platform.insights.api.v1.InsightsServiceGrpc;
import ai.traceable.sensitivedata.config.service.ConfigServiceCoordinator.DataClassificationRuleState;
import ai.traceable.sensitivedata.config.service.SensitiveDataServiceConfig.GrpcClientConfig;
import ai.traceable.sensitivedata.config.service.v1.DropUnparsedJsonPolicy;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigRequest;
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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.config.objectstore.ClientConfig;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.test.MockGenericConfigService;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PiiFilterConfigServiceImplTest {

  private static final String FIRST_DEFAULT_RULE_WITH_REDACTION_ID = "default-1";
  private static final String FIRST_DEFAULT_RULE_WITH_OBFUSCATION_ID = "default-2";
  private static final String SECOND_DEFAULT_RULE_WITH_OBFUSCATION_ID = "default-session-id-1";

  PiiFilterConfigServiceBlockingStub piiFilterStub;
  MockGenericConfigService mockGenericConfigService;
  SensitiveDataServiceConfig mockConfig;
  DataClassificationRuleState dataClassificationRuleState;

  @BeforeEach
  void setUp() {
    mockGenericConfigService = new MockGenericConfigService().mockUpsert().mockGet().mockGetAll();

    mockConfig = mock(SensitiveDataServiceConfig.class);
    when(mockConfig.defaultParamTypeRedactionStrategy()).thenReturn(REDACTION_STRATEGY_HASH);
    when(mockConfig.defaultAutomaticRedactionStrategy()).thenReturn(true);
    when(mockConfig.defaultPiiFilterConfig())
        .thenReturn(
            PiiFilterConfig.newBuilder()
                .addKeyRegexs(PiiElement.newBuilder().setRegex("secret-1"))
                .build());
    when(mockConfig.defaultClientConfig()).thenReturn(ClientConfig.DEFAULT);
    when(mockConfig.insightsClientConfig())
        .thenReturn(GrpcClientConfig.builder().timeout(Duration.ofSeconds(10)).build());
    when(mockConfig.defaultInvalidJsonPolicy())
        .thenReturn(
            InvalidJsonPolicy.newBuilder()
                .setDropUnparsedJsonPolicy(DropUnparsedJsonPolicy.getDefaultInstance())
                .build());
  }

  @AfterEach
  void afterEach() {
    mockGenericConfigService.shutdown();
  }

  @Test
  void testPiiFilterConfigForDataTypes() {
    this.dataClassificationRuleState =
        new DataClassificationRuleState(
            getTestDataTypes(),
            Set.of(
                FIRST_DEFAULT_RULE_WITH_REDACTION_ID,
                FIRST_DEFAULT_RULE_WITH_OBFUSCATION_ID,
                SECOND_DEFAULT_RULE_WITH_OBFUSCATION_ID),
            true,
            true);
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // prepopulated rules + mock insights service rules + rules from datatypes
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
  void testPiiFilterConfigForDataTypesWithoutLegacyDataTypes() {
    this.dataClassificationRuleState =
        new DataClassificationRuleState(getTestDataTypes(), Set.of(), false, false);
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // prepopulated rules + mock insights service rules + rules from datatypes
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
  void testPiiFilterConfigForDataTypesWithoutCustomDataTypes() {
    this.dataClassificationRuleState =
        new DataClassificationRuleState(
            List.of(),
            Set.of(
                FIRST_DEFAULT_RULE_WITH_REDACTION_ID,
                FIRST_DEFAULT_RULE_WITH_OBFUSCATION_ID,
                SECOND_DEFAULT_RULE_WITH_OBFUSCATION_ID),
            true,
            true);
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // prepopulated rules + mock insights service rules + rules from datatypes
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
  void testPiiFilterConfigForDataTypesWithoutSensitiveHeaders() {
    this.dataClassificationRuleState =
        new DataClassificationRuleState(
            getTestDataTypes(),
            Set.of(
                FIRST_DEFAULT_RULE_WITH_REDACTION_ID,
                FIRST_DEFAULT_RULE_WITH_OBFUSCATION_ID,
                SECOND_DEFAULT_RULE_WITH_OBFUSCATION_ID),
            false,
            true);
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // prepopulated rules + mock insights service rules + rules from datatypes
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
  void testPiiFilterConfigForDataTypesWithoutAutoSecretRedaction() {
    this.dataClassificationRuleState =
        new DataClassificationRuleState(
            getTestDataTypes(),
            Set.of(
                FIRST_DEFAULT_RULE_WITH_REDACTION_ID,
                FIRST_DEFAULT_RULE_WITH_OBFUSCATION_ID,
                SECOND_DEFAULT_RULE_WITH_OBFUSCATION_ID),
            true,
            false);
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // prepopulated rules + mock insights service rules + rules from datatypes
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
  void testPiiFilterConfigForDataTypesWithoutLegacyRedact() {
    this.dataClassificationRuleState =
        new DataClassificationRuleState(
            getTestDataTypes(),
            Set.of(FIRST_DEFAULT_RULE_WITH_OBFUSCATION_ID, SECOND_DEFAULT_RULE_WITH_OBFUSCATION_ID),
            true,
            true);
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // prepopulated rules + mock insights service rules + rules from datatypes
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
  void testPiiFilterConfigForDataTypesWithoutLegacyObfuscate() {
    this.dataClassificationRuleState =
        new DataClassificationRuleState(
            getTestDataTypes(), Set.of(FIRST_DEFAULT_RULE_WITH_REDACTION_ID), true, true);
    setupPiiFilterConfigServiceImpl(false, getDefaultRedactionRules());

    PiiFilterConfig piiFilterConfig =
        piiFilterStub
            .getPiiFilterConfig(GetPiiFilterConfigRequest.newBuilder().build())
            .getPiiFilterConfig();
    // prepopulated rules + mock insights service rules + rules from datatypes
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
            FIRST_DEFAULT_RULE_WITH_REDACTION_ID,
            NewRedactionRule.newBuilder()
                .setName("Rule 1")
                .setMatchType(MatchType.MATCH_TYPE_KEY)
                .setRegex("abc123")
                .setRedactionStrategy(REDACTION_STRATEGY_REDACT)
                .build(),
            FIRST_DEFAULT_RULE_WITH_OBFUSCATION_ID,
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
            SECOND_DEFAULT_RULE_WITH_OBFUSCATION_ID,
            NewRedactionRule.newBuilder()
                .setName("Rule 4")
                .setMatchType(MatchType.MATCH_TYPE_HEADER)
                .setRegex("session-id")
                .setRedactionStrategy(REDACTION_STRATEGY_HASH)
                .setSessionIdentifier(true)
                .build());

    return new DefaultRedactionRules(defaultRules);
  }

  private List<DataType> getTestDataTypes() {
    DataType dataType1 =
        DataType.newBuilder()
            .setId("datatype-1")
            .setRule(
                DataTypeRule.newBuilder()
                    .setName("datatyperule-1")
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
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
                    .setDataSuppression(DataSuppression.DATA_SUPPRESSION_REDACT)
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
    return List.of(dataType1, dataType2);
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
                    new DefaultRedactionRulePersistenceStatusStore(
                        configServiceBlockingStub, configChangeEventGenerator),
                    DataClassificationConfigServiceGrpc.newBlockingStub(channel)),
                new InsightsServiceCoordinatorImpl(
                    InsightsServiceGrpc.newBlockingStub(channel), mockConfig),
                new UuidGenerator()))
        .addService(new MockDataClassificationConfigService())
        .start();

    piiFilterStub = PiiFilterConfigServiceGrpc.newBlockingStub(channel);
  }

  class MockDataClassificationConfigService extends DataClassificationConfigServiceImplBase {

    @Override
    public void getDataTypes(
        GetDataTypesRequest request, StreamObserver<GetDataTypesResponse> responseObserver) {
      GetDataTypesResponse.Builder responseBuilder = GetDataTypesResponse.newBuilder();

      responseBuilder.addAllDataTypes(dataClassificationRuleState.getEnabledNonLegacyDataTypes());
      if (dataClassificationRuleState.isLegacySensitiveHeadersEnabled()) {
        responseBuilder.addDataTypes(
            DataType.newBuilder().setId(LEGACY_SENSITIVE_HEADERS_DATA_TYPE_ID));
      }
      if (dataClassificationRuleState.isLegacyAutoRedactionEnabled()) {
        responseBuilder.addDataTypes(
            DataType.newBuilder().setId(LEGACY_AUTOMATIC_SECRET_REDACTION_DATA_TYPE_ID));
      }
      dataClassificationRuleState
          .getEnabledLegacyRedactionRuleIds()
          .forEach(id -> responseBuilder.addDataTypes(DataType.newBuilder().setId(id)));
      responseObserver.onNext(responseBuilder.build());
      responseObserver.onCompleted();
    }
  }
}
