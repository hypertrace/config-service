package ai.traceable.config.service;

import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_REQUEST_BODY;
import static ai.traceable.data.classification.config.service.v1.DataTypeRule.Location.LOCATION_REQUEST_HEADER;
import static com.google.common.io.Resources.getResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.traceable.data.classification.config.service.v1.CreateDataSetRequest;
import ai.traceable.data.classification.config.service.v1.CreateDataTypeRequest;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc;
import ai.traceable.data.classification.config.service.v1.DataClassificationConfigServiceGrpc.DataClassificationConfigServiceBlockingStub;
import ai.traceable.data.classification.config.service.v1.DataSet;
import ai.traceable.data.classification.config.service.v1.DataSetInfo;
import ai.traceable.data.classification.config.service.v1.DataSetInfo.DataSuppression;
import ai.traceable.data.classification.config.service.v1.DataType;
import ai.traceable.data.classification.config.service.v1.DataTypeRule;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Action;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.GlobalScope;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Location;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.Operator;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.ScopedPattern;
import ai.traceable.data.classification.config.service.v1.DataTypeRule.StringPattern;
import ai.traceable.data.classification.config.service.v1.DeleteDataSetRequest;
import ai.traceable.sensitivedata.config.service.v1.CreateRedactionRuleRequest;
import ai.traceable.sensitivedata.config.service.v1.DropUnparsedJsonPolicy;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesRequest;
import ai.traceable.sensitivedata.config.service.v1.GetAllRedactionRulesResponse;
import ai.traceable.sensitivedata.config.service.v1.GetAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.GetFullPrivacyModeRequest;
import ai.traceable.sensitivedata.config.service.v1.GetInvalidJsonPolicyRequest;
import ai.traceable.sensitivedata.config.service.v1.GetPiiFilterConfigRequest;
import ai.traceable.sensitivedata.config.service.v1.GetRedactionStrategyForTypeRequest;
import ai.traceable.sensitivedata.config.service.v1.InvalidJsonPolicy;
import ai.traceable.sensitivedata.config.service.v1.MatchType;
import ai.traceable.sensitivedata.config.service.v1.NewRedactionRule;
import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.PiiElement;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfigServiceGrpc.PiiFilterConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.RedactionRule;
import ai.traceable.sensitivedata.config.service.v1.RedactionStrategy;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc;
import ai.traceable.sensitivedata.config.service.v1.SensitiveDataConfigServiceGrpc.SensitiveDataConfigServiceBlockingStub;
import ai.traceable.sensitivedata.config.service.v1.UpdateAutomaticSecretRedactionStrategyRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateFullPrivacyModeRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateInvalidJsonPolicyRequest;
import ai.traceable.sensitivedata.config.service.v1.UpdateRedactionStrategyForTypeRequest;
import com.google.common.io.Resources;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration test for SensitiveDataConfigService */
class SensitiveDataConfigServiceIntegrationTest extends TraceableConfigServiceIntegrationTestBase {

  private static SensitiveDataConfigServiceBlockingStub sensitiveDataConfigServiceStub;
  private static PiiFilterConfigServiceBlockingStub piiFilterConfigServiceStub;
  private static DataClassificationConfigServiceBlockingStub dataClassificationConfigServiceStub;
  private RequestContext requestContext;

  @BeforeAll
  static void init() {
    sensitiveDataConfigServiceStub =
        SensitiveDataConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    piiFilterConfigServiceStub =
        PiiFilterConfigServiceGrpc.newBlockingStub(managedChannelForExternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
    dataClassificationConfigServiceStub =
        DataClassificationConfigServiceGrpc.newBlockingStub(managedChannelForInternalServices)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  @Test
  void testSensitiveDataConfigService() {
    requestContext = RequestContext.forTenantId("testSensitiveDataConfigService-tenant");
    assertEquals(
        RedactionStrategy.REDACTION_STRATEGY_RAW,
        getRedactionStrategyForType(ParamType.PARAM_TYPE_HEADER));
    updateRedactionStrategyForType(
        ParamType.PARAM_TYPE_HEADER, RedactionStrategy.REDACTION_STRATEGY_HASH);
    assertEquals(
        RedactionStrategy.REDACTION_STRATEGY_HASH,
        getRedactionStrategyForType(ParamType.PARAM_TYPE_HEADER));

    assertTrue(getAutomaticSecretRedactionStrategy());
    updateAutomaticSecretRedactionStrategy(false);
    assertFalse(getAutomaticSecretRedactionStrategy());

    assertFalse(getInvalidJsonPolicy().hasDropUnparsedJsonPolicy());
    updateInvalidJsonPolicy(
        InvalidJsonPolicy.newBuilder()
            .setDropUnparsedJsonPolicy(DropUnparsedJsonPolicy.getDefaultInstance())
            .build());
    assertTrue(getInvalidJsonPolicy().hasDropUnparsedJsonPolicy());
  }

  @Test
  void testPiiFilterConfigService() {
    requestContext = RequestContext.forTenantId("testPiiFilterConfigService-tenant");
    // automatic secret redaction is enabled and redaction strategy is set to RAW(default)
    assertMatchesResource(
        "sensitive-data/pii-filter-with-auto-redaction.json", getPiiFilterConfig(false));

    // set redaction strategy to HASH
    updateRedactionStrategyForType(
        ParamType.PARAM_TYPE_HEADER, RedactionStrategy.REDACTION_STRATEGY_HASH);
    assertMatchesResource(
        "sensitive-data/pii-filter-with-hash-strategy.json", getPiiFilterConfig(false));

    // disable automatic secret redaction
    updateAutomaticSecretRedactionStrategy(false);
    updateInvalidJsonPolicy(
        InvalidJsonPolicy.newBuilder()
            .setDropUnparsedJsonPolicy(DropUnparsedJsonPolicy.getDefaultInstance())
            .build());
    assertMatchesResource(
        "sensitive-data/pii-filter-without-auto-redaction.json", getPiiFilterConfig(false));
    updateAutomaticSecretRedactionStrategy(true);
    updateInvalidJsonPolicy(InvalidJsonPolicy.getDefaultInstance());
    assertMatchesResource(
        "sensitive-data/pii-filter-with-auto-redaction-again.json", getPiiFilterConfig(false));

    // add redaction rule
    createRedactionRule(
        getNewRedactionRule("rule-1", RedactionStrategy.REDACTION_STRATEGY_REDACT, "^name", false));
    createRedactionRule(
        getNewRedactionRule("rule-2", RedactionStrategy.REDACTION_STRATEGY_HASH, "^address", true));
    assertMatchesResource("sensitive-data/pii-filter-after-add.json", getPiiFilterConfig(false));

    // Test for full privacy mode
    // When disabled, which is the default, the full privacy mode rules should not be sent down.
    assertMatchesResource(
        "sensitive-data/pii-filter-with-full-privacy-mode-disabled.json", getPiiFilterConfig(true));

    // When disabled, which is the default, the full privacy mode rules should be sent down.
    updateFullPrivacyMode(true);
    assertMatchesResource(
        "sensitive-data/pii-filter-with-full-privacy-mode-enabled.json", getPiiFilterConfig(true));

    updateFullPrivacyMode(false);
    assertMatchesResource(
        "sensitive-data/pii-filter-with-full-privacy-mode-disabled.json", getPiiFilterConfig(true));
  }

  @Test
  void testDefaultPopulationConcurrency() throws InterruptedException {
    requestContext = RequestContext.forTenantId("testDefaultPopulationConcurrency-tenant");
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    List<Callable<GetAllRedactionRulesResponse>> calls =
        Stream.generate(
                () -> (Callable<GetAllRedactionRulesResponse>) () -> this.getRedactionRules(true))
            .limit(10)
            .collect(Collectors.toList());
    List<Future<GetAllRedactionRulesResponse>> responses = executorService.invokeAll(calls);

    responses.forEach(
        response -> {
          try {
            assertMatchesResource(
                "sensitive-data/get-all-redaction-rules-prepop-response.json", response.get());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    calls =
        Stream.generate(
                () -> (Callable<GetAllRedactionRulesResponse>) () -> this.getRedactionRules(false))
            .limit(10)
            .collect(Collectors.toList());
    responses = executorService.invokeAll(calls);

    responses.forEach(
        response -> {
          try {
            assertMatchesResource(
                "sensitive-data/get-all-redaction-rules-unpersisted-response.json", response.get());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    calls =
        Stream.generate(() -> (Callable<GetAllRedactionRulesResponse>) this::getAllRedactionRules)
            .limit(10)
            .collect(Collectors.toList());
    responses = executorService.invokeAll(calls);

    responses.forEach(
        response -> {
          try {
            assertMatchesResource(
                "sensitive-data/get-all-redaction-rules-response.json", response.get());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  void testFullPrivacyMode() {
    requestContext = RequestContext.forTenantId("testFullPrivacyMode-tenant");

    // Default full privacy mode should be false
    assertFalse(getFullPrivacyMode());

    // Update to true
    updateFullPrivacyMode(true);
    assertTrue(getFullPrivacyMode());

    // Update to false
    updateFullPrivacyMode(false);
    assertFalse(getFullPrivacyMode());
  }

  @Test
  void testConvertDataTypeRuleToPiiElementConfig() {
    requestContext = RequestContext.forTenantId("testConvertDataTypeRuleToPiiElement");
    DataType dataType1 = createDataType("datatype-rule-1", LOCATION_REQUEST_HEADER, "^header");
    DataType dataType2 = createDataType("datatype-rule-2", LOCATION_REQUEST_BODY, "^doby");
    DataType dataType3 = createDataType("datatype-rule-3", LOCATION_REQUEST_BODY, "body");

    createDataSet(
        "dataset-1",
        List.of(dataType1.getId(), dataType2.getId()),
        DataSuppression.DATA_SUPPRESSION_REDACT);
    createDataSet(
        "dataset-2",
        List.of(dataType2.getId(), dataType3.getId()),
        DataSuppression.DATA_SUPPRESSION_OBFUSCATE);

    updateInvalidJsonPolicy(
        InvalidJsonPolicy.newBuilder()
            .setDropUnparsedJsonPolicy(DropUnparsedJsonPolicy.getDefaultInstance())
            .build());

    assertMatchesResource(
        "sensitive-data/pii-filter-with-data-type-rules.json", getPiiFilterConfig(false));
  }

  @Test
  void testPiiFilterWithLegacyAutoSecretRedactionDataSetDeleted() {
    requestContext =
        RequestContext.forTenantId(
            "testPiiFilterWithLegacyAutoSecretRedactionDataSetDeleted-tenant");
    // automatic secret redaction is enabled and redaction strategy is set to RAW(default)
    assertMatchesResource(
        "sensitive-data/pii-filter-with-auto-redaction.json", getPiiFilterConfig(false));

    // delete legacy automatic secret redaction data set
    requestContext.call(
        () ->
            dataClassificationConfigServiceStub.deleteDataSet(
                DeleteDataSetRequest.newBuilder()
                    .setId("legacy-dataset-automatic-secret-redaction-id")
                    .build()));

    assertMatchesResource(
        "sensitive-data/pii-filter-without-auto-redaction-without-hash.json",
        getPiiFilterConfig(false));

    updateAutomaticSecretRedactionStrategy(true);
  }

  @Test
  void testPiiFilterWithLegacySensitiveHeadersDataSetDeleted() {
    requestContext =
        RequestContext.forTenantId("testPiiFilterWithLegacySensitiveHeadersDataSetDeleted-tenant");

    // set redaction strategy to HASH
    updateRedactionStrategyForType(
        ParamType.PARAM_TYPE_HEADER, RedactionStrategy.REDACTION_STRATEGY_HASH);
    assertMatchesResource(
        "sensitive-data/pii-filter-with-hash-strategy.json", getPiiFilterConfig(false));

    // delete legacy sensitive headers data set
    requestContext.call(
        () ->
            dataClassificationConfigServiceStub.deleteDataSet(
                DeleteDataSetRequest.newBuilder()
                    .setId("legacy-dataset-sensitive-headers-id")
                    .build()));

    assertMatchesResource(
        "sensitive-data/pii-filter-with-auto-redaction.json", getPiiFilterConfig(false));

    updateRedactionStrategyForType(
        ParamType.PARAM_TYPE_HEADER, RedactionStrategy.REDACTION_STRATEGY_RAW);
  }

  private DataSet createDataSet(
      String name, List<String> datatypeIds, DataSuppression dataSuppression) {
    CreateDataSetRequest request =
        CreateDataSetRequest.newBuilder()
            .setInfo(
                DataSetInfo.newBuilder()
                    .setName(name)
                    .setEnabled(true)
                    .setDataSuppression(dataSuppression)
                    .addAllDataTypeIds(datatypeIds))
            .build();

    return requestContext
        .call(() -> dataClassificationConfigServiceStub.createDataSet(request))
        .getDataSet();
  }

  private DataType createDataType(String name, Location location, String key) {
    CreateDataTypeRequest request =
        CreateDataTypeRequest.newBuilder()
            .setRule(
                DataTypeRule.newBuilder()
                    .setName(name)
                    .addScopedPatterns(
                        ScopedPattern.newBuilder()
                            .setGlobalScope(GlobalScope.newBuilder())
                            .addLocations(location)
                            .setKeyPattern(
                                StringPattern.newBuilder()
                                    .setValue(key)
                                    .setOperator(Operator.OPERATOR_MATCHES_REGEX))
                            .setAction(Action.ACTION_MATCH)))
            .build();

    return requestContext
        .call(() -> dataClassificationConfigServiceStub.createDataType(request))
        .getDataType();
  }

  private void updateRedactionStrategyForType(
      ParamType paramType, RedactionStrategy redactionStrategy) {
    UpdateRedactionStrategyForTypeRequest request =
        UpdateRedactionStrategyForTypeRequest.newBuilder()
            .setParamType(paramType)
            .setRedactionStrategy(redactionStrategy)
            .build();
    requestContext.call(
        () -> sensitiveDataConfigServiceStub.updateRedactionStrategyForType(request));
  }

  private RedactionStrategy getRedactionStrategyForType(ParamType paramType) {
    GetRedactionStrategyForTypeRequest request =
        GetRedactionStrategyForTypeRequest.newBuilder().setParamType(paramType).build();
    return requestContext
        .call(() -> sensitiveDataConfigServiceStub.getRedactionStrategyForType(request))
        .getRedactionStrategy();
  }

  private void updateFullPrivacyMode(boolean fullPrivacyMode) {
    UpdateFullPrivacyModeRequest request =
        UpdateFullPrivacyModeRequest.newBuilder().setEnabled(fullPrivacyMode).build();
    requestContext.call(() -> sensitiveDataConfigServiceStub.updateFullPrivacyMode(request));
  }

  private boolean getFullPrivacyMode() {
    GetFullPrivacyModeRequest request = GetFullPrivacyModeRequest.newBuilder().build();
    return requestContext
        .call(() -> sensitiveDataConfigServiceStub.getFullPrivacyMode(request))
        .getEnabled();
  }

  private void updateAutomaticSecretRedactionStrategy(boolean enabled) {
    UpdateAutomaticSecretRedactionStrategyRequest request =
        UpdateAutomaticSecretRedactionStrategyRequest.newBuilder().setEnabled(enabled).build();
    requestContext.call(
        () -> sensitiveDataConfigServiceStub.updateAutomaticSecretRedactionStrategy(request));
  }

  private void updateInvalidJsonPolicy(InvalidJsonPolicy invalidJsonPolicy) {
    UpdateInvalidJsonPolicyRequest request =
        UpdateInvalidJsonPolicyRequest.newBuilder().setInvalidJsonPolicy(invalidJsonPolicy).build();
    requestContext.call(() -> sensitiveDataConfigServiceStub.updateInvalidJsonPolicy(request));
  }

  private boolean getAutomaticSecretRedactionStrategy() {
    GetAutomaticSecretRedactionStrategyRequest request =
        GetAutomaticSecretRedactionStrategyRequest.newBuilder().build();
    return requestContext
        .call(() -> sensitiveDataConfigServiceStub.getAutomaticSecretRedactionStrategy(request))
        .getEnabled();
  }

  private InvalidJsonPolicy getInvalidJsonPolicy() {
    GetInvalidJsonPolicyRequest request = GetInvalidJsonPolicyRequest.newBuilder().build();
    return requestContext
        .call(() -> sensitiveDataConfigServiceStub.getInvalidJsonPolicy(request))
        .getInvalidJsonPolicy();
  }

  private RedactionRule createRedactionRule(NewRedactionRule newRedactionRule) {
    CreateRedactionRuleRequest request =
        CreateRedactionRuleRequest.newBuilder().setNewRedactionRule(newRedactionRule).build();
    return requestContext
        .call(() -> sensitiveDataConfigServiceStub.createRedactionRule(request))
        .getRedactionRule();
  }

  private GetAllRedactionRulesResponse getRedactionRules(boolean isPersisted) {
    return requestContext.call(
        () ->
            sensitiveDataConfigServiceStub.getAllRedactionRules(
                GetAllRedactionRulesRequest.newBuilder()
                    .setFilter(
                        GetAllRedactionRulesRequest.RedactionRuleFilter.newBuilder()
                            .setIsPersisted(isPersisted)
                            .build())
                    .build()));
  }

  private GetAllRedactionRulesResponse getAllRedactionRules() {
    return requestContext.call(
        () ->
            sensitiveDataConfigServiceStub.getAllRedactionRules(
                GetAllRedactionRulesRequest.getDefaultInstance()));
  }

  private NewRedactionRule getNewRedactionRule(
      String name, RedactionStrategy redactionStrategy, String regex, boolean sessionIdentifier) {
    return NewRedactionRule.newBuilder()
        .setName(name)
        .setDescription("sample rule")
        .setCategory("pii")
        .setRedactionStrategy(redactionStrategy)
        .setMatchType(MatchType.MATCH_TYPE_KEY)
        .setRegex(regex)
        .setSessionIdentifier(sessionIdentifier)
        .build();
  }

  private PiiFilterConfig getPiiFilterConfig(boolean includeConditionalRules) {
    GetPiiFilterConfigRequest request =
        GetPiiFilterConfigRequest.newBuilder()
            .setIncludeConditionalRules(includeConditionalRules)
            .build();
    return requestContext
        .call(() -> piiFilterConfigServiceStub.getPiiFilterConfig(request))
        .getPiiFilterConfig();
  }

  private void assertMatchesResource(String resourcePath, PiiFilterConfig config) {
    PiiFilterConfig.Builder builder = PiiFilterConfig.newBuilder();
    try {
      String json = Resources.toString(getResource(resourcePath), StandardCharsets.UTF_8);
      JsonFormat.parser().merge(json, builder);
      String expected = toJsonWithIdPlaceholders(builder.build());
      String actual = toJsonWithIdPlaceholders(config);
      assertEquals(expected, actual);
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  private void assertMatchesResource(String resourcePath, GetAllRedactionRulesResponse config) {
    GetAllRedactionRulesResponse.Builder builder = GetAllRedactionRulesResponse.newBuilder();
    try {
      String json = Resources.toString(getResource(resourcePath), StandardCharsets.UTF_8);
      JsonFormat.parser().merge(json, builder);
      String expected = toJsonWithIdPlaceholders(builder.build());
      String actual = toJsonWithIdPlaceholders(config);
      assertEquals(expected, actual);
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  private String toJsonWithIdPlaceholders(PiiFilterConfig config)
      throws InvalidProtocolBufferException {
    // This replaces any IDs to placeholders, since they are generated each test
    PiiFilterConfig.Builder updatedBuilder = config.toBuilder();

    updatedBuilder.clearKeyRegexs();
    updatedBuilder.addAllKeyRegexs(
        config.getKeyRegexsList().stream()
            .map(this::usePlaceholderIdIfNeeded)
            .collect(Collectors.toUnmodifiableList()));

    updatedBuilder.clearValueRegexs();
    updatedBuilder.addAllValueRegexs(
        config.getValueRegexsList().stream()
            .map(this::usePlaceholderIdIfNeeded)
            .collect(Collectors.toUnmodifiableList()));

    return JsonFormat.printer().print(updatedBuilder);
  }

  private String toJsonWithIdPlaceholders(GetAllRedactionRulesResponse response)
      throws InvalidProtocolBufferException {
    // This replaces any IDs to placeholders, since they are generated each test
    GetAllRedactionRulesResponse.Builder updatedBuilder = response.toBuilder();

    updatedBuilder.clearRedactionRules();
    updatedBuilder.addAllRedactionRules(
        response.getRedactionRulesList().stream()
            .map(redactionRule -> redactionRule.toBuilder().setId("placeholder-id").build())
            .collect(Collectors.toUnmodifiableList()));

    return JsonFormat.printer().print(updatedBuilder);
  }

  private PiiElement usePlaceholderIdIfNeeded(PiiElement piiElement) {
    if (piiElement.getRuleId().isBlank()) {
      return piiElement;
    }
    return piiElement.toBuilder().setRuleId("placeholder-id").build();
  }
}
